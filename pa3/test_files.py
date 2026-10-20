"""Tests for framing, discovery messages, file lists, and file transfer.

File bytes are the raw TCP payload. They are not Base64 inside the JSON
header: that header is limited to 64 KB, and Part 1 already sends the bytes
after it.
"""

import json
import os
import socket
import struct
import tempfile
import threading
import time

import connection
import fileshare
import logger
from discovery import (
    Discovery,
    interpret_udp,
    parse_udp_message,
    peer_ack_message,
    peer_request_message,
)
from peers import Peer, PeerTable
from protocol import LENGTH_FORMAT, recv_msg, send_msg, set_node_id

ID_A = "aaaaaaaa-0000-0000-0000-000000000000"
ID_B = "bbbbbbbb-0000-0000-0000-000000000000"
ID_C = "cccccccc-0000-0000-0000-000000000000"

# unit-test logs stay out of the project log.txt
logger.LOG_FILE = os.path.join(tempfile.mkdtemp(prefix="pa3-unit-log-"), "log.txt")
set_node_id("test-node")


class MemorySocket:
    """A stand-in socket. recv returns a few bytes at a time."""

    def __init__(self, incoming=b"", chunk=1):
        self.incoming = incoming
        self.chunk = chunk
        self.sent = b""
        self._pos = 0

    def sendall(self, data):
        self.sent += data

    def recv(self, n):
        if self._pos >= len(self.incoming):
            return b""
        end = min(len(self.incoming), self._pos + min(n, self.chunk))
        data = self.incoming[self._pos:end]
        self._pos = end
        return data


class FakePeer:
    def __init__(self, node_id="node2"):
        self.node_id = node_id
        self.addr = ("127.0.0.1", 5002)
        self.files = []
        self.table = None
        self.sent = []
        self.closed = False

    def send(self, msg, payload=b""):
        self.sent.append((msg, payload))
        return True

    def disconnect(self):
        self.closed = True


def wait_for(condition, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False


def use_temp_dirs():
    root = tempfile.mkdtemp(prefix="pa3-files-")
    shared = os.path.join(root, "shared")
    downloads = os.path.join(root, "downloads")
    fileshare.configure_dirs(shared, downloads)
    return shared, downloads


def hello(my_id, files):
    # "from" is set here because one process can only have one default node id
    return lambda peer: peer.send({"type": "file_list", "from": my_id, "files": files})


def connect_pair(table_a, table_b, files_a, files_b):
    listener = socket.create_server(("127.0.0.1", 0))
    client = socket.create_connection(listener.getsockname())
    server, addr = listener.accept()
    connection.start_peer_thread(server, addr, table_a, False, hello(ID_A, files_a))
    peer_b = connection.start_peer_thread(
        client, client.getpeername(), table_b, True, hello(ID_B, files_b)
    )
    return listener, peer_b


def test_message_encode_decode():
    writer = MemorySocket()
    send_msg(writer, {"type": "file_list", "files": ["a.txt", "b.txt"]}, b"", ("127.0.0.1", 5001))
    header, payload = recv_msg(MemorySocket(writer.sent, chunk=len(writer.sent)))
    assert header["type"] == "file_list"
    assert header["files"] == ["a.txt", "b.txt"]
    assert header["from"] == "test-node"
    assert header["payload_len"] == 0
    assert payload == b""
    print("ok: message encode and decode")


def test_length_prefix_framing():
    writer = MemorySocket()
    payload = b"hello-bytes"
    send_msg(writer, {"type": "file_data", "filename": "a.txt", "success": True}, payload)
    raw = writer.sent
    header_len = struct.unpack(LENGTH_FORMAT, raw[:4])[0]
    header = json.loads(raw[4:4 + header_len].decode("utf-8"))
    body = raw[4 + header_len:]
    assert header["payload_len"] == len(payload)
    assert body == payload
    # one recv of the whole buffer is not required; the length tells us where it ends
    decoded, got = recv_msg(MemorySocket(raw, chunk=3))
    assert decoded["filename"] == "a.txt"
    assert got == payload
    print("ok: 4-byte big-endian length prefix frames the header")


def test_partial_tcp_receives():
    writer = MemorySocket()
    payload = os.urandom(1000)
    send_msg(writer, {"type": "file_data", "filename": "blob.bin", "success": True}, payload)
    # one byte per recv, including a length prefix split across reads
    header, got = recv_msg(MemorySocket(writer.sent, chunk=1))
    assert header["type"] == "file_data"
    assert got == payload
    print("ok: partial TCP receives are reassembled")


def test_file_bytes_are_raw_payload():
    writer = MemorySocket()
    original = b"not-base64 \x00\xff"
    send_msg(writer, {"type": "file_data", "filename": "a.txt", "success": True}, original)
    raw = writer.sent
    header_len = struct.unpack(LENGTH_FORMAT, raw[:4])[0]
    header_bytes = raw[4:4 + header_len]
    header = json.loads(header_bytes.decode("utf-8"))
    # the JSON header has no Base64 "data" field; the bytes sit after it
    assert "data" not in header
    assert original not in header_bytes
    assert raw[4 + header_len:] == original
    print("ok: file bytes are the raw payload, not Base64 inside JSON")


def test_peer_request_and_ack_handling():
    request = peer_request_message("node1")
    assert request == {"type": "peer_request", "from": "node1"}
    assert interpret_udp(request, "node2") == "ack"
    assert interpret_udp(request, "node1") == "ignore"

    ack = peer_ack_message("node2", "node1", 5002)
    assert ack["tcp_port"] == 5002
    assert interpret_udp(ack, "node1") == "connect"
    assert interpret_udp(ack, "node2") == "ignore"
    # an ACK aimed at someone else is not a reason to connect
    assert interpret_udp(peer_ack_message("node2", "node3", 5002), "node1") == "ignore"
    assert interpret_udp(peer_ack_message("node2", "node1", True), "node1") == "ignore"
    assert interpret_udp(peer_ack_message("node2", "node1", 0), "node1") == "ignore"
    assert interpret_udp(parse_udp_message(b"not-json"), "node1") == "malformed"
    assert parse_udp_message(b"not-json") is None
    print("ok: peer_request and peer_ack handling")


def test_udp_discovery_exchange():
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    probe.bind(("", 0))
    port = probe.getsockname()[1]
    probe.close()

    dialed = []
    table_a, table_b = PeerTable("nodeA"), PeerTable("nodeB")

    def remember(which):
        def dial(ip, tcp_port, remote_id, done):
            dialed.append((which, tcp_port, remote_id))
            # leave dialing set so the 10-second repeat does not open a second one
        return dial

    node_a = Discovery("nodeA", 6101, table_a, remember("A"), port)
    node_b = Discovery("nodeB", 6102, table_b, remember("B"), port)
    node_b.start()
    node_a.start()
    try:
        assert wait_for(lambda: ("A", 6102, "nodeB") in dialed, 8), dialed
        assert wait_for(lambda: ("B", 6101, "nodeA") in dialed, 8), dialed
    finally:
        node_a.stop()
        node_b.stop()
    text = open(logger.LOG_FILE, encoding="utf-8").read()
    assert "SENT peer_request to 255.255.255.255:" in text
    assert "RECEIVED peer_ack from node" in text
    print("ok: UDP discovery exchanges peer_request and peer_ack")


def test_file_list_creation():
    shared, _downloads = use_temp_dirs()
    with open(os.path.join(shared, "b.txt"), "w", encoding="utf-8") as handle:
        handle.write("b")
    with open(os.path.join(shared, "a.txt"), "w", encoding="utf-8") as handle:
        handle.write("a")
    os.makedirs(os.path.join(shared, "subdir"))
    with open(os.path.join(shared, "subdir", "hidden.txt"), "w", encoding="utf-8") as handle:
        handle.write("nope")
    with open(os.path.join(shared, ".temp.part"), "w", encoding="utf-8") as handle:
        handle.write("partial")
    assert fileshare.list_shared_files() == ["a.txt", "b.txt"]
    assert fileshare.file_list_message()["files"] == ["a.txt", "b.txt"]
    assert fileshare.is_safe_filename("../secret.txt") is False
    assert fileshare.is_safe_filename("/etc/passwd") is False
    assert fileshare.is_safe_filename("ok.txt") is True
    print("ok: file list is regular filenames only")


def test_file_list_exchange_and_removal():
    shared, _downloads = use_temp_dirs()
    with open(os.path.join(shared, "offered.txt"), "w", encoding="utf-8") as handle:
        handle.write("offered")
    fileshare.register(connection)
    table_a, table_b = PeerTable(ID_A), PeerTable(ID_B)
    listener, peer_b = connect_pair(table_a, table_b, ["from-a.txt"], ["offered.txt", "../secret.txt"])
    assert wait_for(lambda: table_a.files_of(ID_B) == ["offered.txt"]), table_a.snapshot()
    assert wait_for(lambda: table_b.files_of(ID_A) == ["from-a.txt"]), table_b.snapshot()
    # the unsafe name in the advertised list is stored by neither side
    assert "../secret.txt" not in (table_a.files_of(ID_B) or [])

    peer_b.disconnect()
    assert wait_for(lambda: not table_a.has(ID_B)), "A kept B after disconnect"
    assert wait_for(lambda: not table_b.has(ID_A)), "B kept A after disconnect"
    assert table_a.snapshot() == {}
    text = open(logger.LOG_FILE, encoding="utf-8").read()
    assert "peer removed:" in text
    listener.close()
    print("ok: file lists are exchanged, and a disconnect drops them")


def test_data_request_and_missing_file():
    shared, downloads = use_temp_dirs()
    with open(os.path.join(shared, "note.txt"), "w", encoding="utf-8") as handle:
        handle.write("hello from shared")
    outside = os.path.join(os.path.dirname(shared), "secret.txt")
    with open(outside, "w", encoding="utf-8") as handle:
        handle.write("TOP-SECRET")

    owner = FakePeer("node2")
    fileshare.handle_data_request(owner, {"type": "data_request", "filename": "note.txt"}, b"")
    message, payload = owner.sent[-1]
    assert message["success"] is True
    assert message["filename"] == "note.txt"
    assert payload == b"hello from shared"

    fileshare.handle_data_request(owner, {"type": "data_request", "filename": "missing.txt"}, b"")
    message, payload = owner.sent[-1]
    assert message == {
        "type": "file_data",
        "filename": "missing.txt",
        "success": False,
        "error": "File not found",
    }
    assert payload == b""
    assert owner.closed is False

    fileshare.handle_data_request(owner, {"type": "data_request", "filename": "../secret.txt"}, b"")
    message, payload = owner.sent[-1]
    assert message["success"] is False
    assert message["error"] == "Invalid filename"
    assert b"TOP-SECRET" not in payload
    assert owner.closed is False

    # the same checks across a real socket, and the connection stays up afterwards
    fileshare.register(connection)
    seen = {}
    arrived = threading.Event()
    real_handler = fileshare.handle_file_data

    def watch(peer, msg, payload):
        if msg.get("success") is False:
            seen["error"] = dict(msg)
            seen["payload"] = payload
            arrived.set()
        return real_handler(peer, msg, payload)

    connection.register_handler("file_data", watch)
    table_a, table_b = PeerTable(ID_A), PeerTable(ID_B)
    listener, peer_b = connect_pair(table_a, table_b, [], [])
    assert wait_for(lambda: table_a.has(ID_B) and table_b.has(ID_A))
    assert peer_b.send({"type": "data_request", "from": ID_B, "filename": "missing.txt"})
    assert arrived.wait(5), "missing-file error never arrived"
    assert seen["error"]["error"] == "File not found"
    assert seen["payload"] == b""
    assert not os.path.exists(os.path.join(downloads, "missing.txt"))

    # a later file list still arrives, so the error did not close the socket
    assert peer_b.send({"type": "file_list", "from": ID_B, "files": ["later.txt"]})
    assert wait_for(lambda: table_a.files_of(ID_B) == ["later.txt"]), table_a.snapshot()
    listener.close()
    connection.register_handler("file_data", real_handler)
    print("ok: data requests, missing files, and path traversal")


def test_save_download_copy_and_duplicate():
    shared, downloads = use_temp_dirs()
    assert fileshare.save_incoming_file("got.txt", b"abc123") is True
    assert open(os.path.join(downloads, "got.txt"), "rb").read() == b"abc123"
    assert open(os.path.join(shared, "got.txt"), "rb").read() == b"abc123"
    # the half-written name must not be left behind for the next file list
    assert not os.path.exists(os.path.join(downloads, ".got.txt.part"))
    assert "got.txt" in fileshare.list_shared_files()

    with open(os.path.join(shared, "got.txt"), "wb") as handle:
        handle.write(b"original")
    assert fileshare.save_incoming_file("got.txt", b"replacement") is False
    assert open(os.path.join(downloads, "got.txt"), "rb").read() == b"replacement"
    assert open(os.path.join(shared, "got.txt"), "rb").read() == b"original"

    assert fileshare.save_incoming_file("../secret.txt", b"nope") is False
    assert not os.path.exists(os.path.join(os.path.dirname(shared), "secret.txt"))
    print("ok: downloads, copy into shared, and no overwrite")


def test_request_rules():
    shared, _downloads = use_temp_dirs()
    with open(os.path.join(shared, "mine.txt"), "w", encoding="utf-8") as handle:
        handle.write("mine")
    table = PeerTable("node1")
    sock = MemorySocket()
    peer = Peer(sock, ("127.0.0.1", 5002), True)
    peer.node_id = "node2"
    assert table.add(peer) is True
    table.set_files(peer, ["theirs.txt"])

    assert "already in shared" in fileshare.request_file(table, "node2", "mine.txt")
    assert sock.sent == b""
    assert "does not advertise" in fileshare.request_file(table, "node2", "other.txt")
    assert sock.sent == b""
    assert "not a plain filename" in fileshare.request_file(table, "node2", "../secret.txt")
    result = fileshare.request_file(table, "node2", "theirs.txt")
    assert result == "requested theirs.txt from node2"
    header, _payload = recv_msg(MemorySocket(sock.sent))
    assert header["type"] == "data_request"
    assert header["filename"] == "theirs.txt"
    print("ok: requests follow the advertised list and skip local files")


def test_invalid_message_does_not_drop_other_peer():
    fileshare.register(connection)
    table = PeerTable(ID_A)
    listener = socket.create_server(("127.0.0.1", 0))

    def client(files):
        sock = socket.create_connection(listener.getsockname())
        server, addr = listener.accept()
        connection.start_peer_thread(server, addr, table, False, hello(ID_A, []))
        # read the hello so the server's send does not block, then answer
        recv_msg(sock)
        send_msg(sock, {"type": "file_list", "from": files, "files": [files + ".txt"]})
        return sock

    good = client(ID_C)
    bad = client(ID_B)
    assert wait_for(lambda: table.has(ID_C) and table.has(ID_B))
    # a length that is not JSON makes this one connection unreadable
    bad.sendall(struct.pack(LENGTH_FORMAT, 4) + b"????")
    assert wait_for(lambda: not table.has(ID_B)), "broken peer was kept"
    assert table.has(ID_C)
    assert wait_for(lambda: table.files_of(ID_C) == [ID_C + ".txt"])
    good.close()
    listener.close()
    print("ok: one bad message removes only that peer")


def test_connection_reset_and_clean_close():
    table = PeerTable(ID_A)
    closed = threading.Event()
    reset = threading.Event()

    class CloseSocket:
        def sendall(self, data):
            return None

        def recv(self, n):
            return b""

        def close(self):
            return None

        def shutdown(self, how):
            return None

    class ResetSocket(CloseSocket):
        def recv(self, n):
            raise ConnectionResetError("reset")

    connection.start_peer_thread(
        CloseSocket(), ("127.0.0.1", 1), table, False, hello(ID_A, []),
        on_finish=lambda peer: closed.set(),
    )
    connection.start_peer_thread(
        ResetSocket(), ("127.0.0.1", 2), table, False, hello(ID_A, []),
        on_finish=lambda peer: reset.set(),
    )
    assert closed.wait(2), "clean close was not handled"
    assert reset.wait(2), "connection reset was not handled"
    text = open(logger.LOG_FILE, encoding="utf-8").read()
    assert "peer closed the connection" in text
    assert "connection reset by" in text
    assert table.snapshot() == {}
    print("ok: clean close and connection reset are caught")


def test_logging_is_thread_safe():
    path = os.path.join(tempfile.mkdtemp(prefix="pa3-log-"), "log.txt")
    previous = logger.LOG_FILE
    logger.LOG_FILE = path
    errors = []

    def write_one(index):
        try:
            logger.log_message("sent", "log-probe", "file_list")
            logger.log_message("received", ("127.0.0.1", 5099), "data_request")
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=write_one, args=(i,)) for i in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    logger.LOG_FILE = previous
    assert errors == []
    lines = open(path, encoding="utf-8").read().splitlines()
    sent = [line for line in lines if "SENT file_list to log-probe" in line]
    received = [line for line in lines if "RECEIVED data_request from 127.0.0.1:5099" in line]
    assert len(sent) == 20
    assert len(received) == 20
    assert all(line[4] == "-" and line[19:20] == " " for line in sent)
    print("ok: log lines are timestamped and thread-safe")


def main():
    test_message_encode_decode()
    test_length_prefix_framing()
    test_partial_tcp_receives()
    test_file_bytes_are_raw_payload()
    test_peer_request_and_ack_handling()
    test_udp_discovery_exchange()
    test_file_list_creation()
    test_file_list_exchange_and_removal()
    test_data_request_and_missing_file()
    test_save_download_copy_and_duplicate()
    test_request_rules()
    test_invalid_message_does_not_drop_other_peer()
    test_connection_reset_and_clean_close()
    test_logging_is_thread_safe()
    print("all file tests passed")


if __name__ == "__main__":
    main()
