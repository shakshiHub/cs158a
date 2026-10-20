import os
import socket
import threading
import time

import connection
import logger
from peers import PeerTable

logger.LOG_FILE = "test_log.txt"

ID_A = "aaaaaaaa-0000-0000-0000-000000000000"
ID_B = "bbbbbbbb-0000-0000-0000-000000000000"


def hello(my_id):
    return lambda peer: peer.send({"type": "file_list", "from": my_id, "files": []})


def wait_for(condition, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False


def connect_pair(listener, table_a, table_b):
    b_sock = socket.create_connection(listener.getsockname())
    a_sock, a_addr = listener.accept()
    peer_on_a = connection.start_peer_thread(a_sock, a_addr, table_a, False, hello(ID_A))
    peer_on_b = connection.start_peer_thread(b_sock, b_sock.getpeername(), table_b, True, hello(ID_B))
    return peer_on_a, peer_on_b


def test_connect_transfer_disconnect():
    received = {}
    got_blob = threading.Event()

    def on_blob(peer, msg, payload):
        received["payload"] = payload
        got_blob.set()

    connection.register_handler("file_list", lambda peer, msg, payload: None)
    connection.register_handler("test_blob", on_blob)

    table_a, table_b = PeerTable(ID_A), PeerTable(ID_B)
    listener = socket.create_server(("127.0.0.1", 0))
    _, peer_on_b = connect_pair(listener, table_a, table_b)

    assert wait_for(lambda: table_a.has(ID_B) and table_b.has(ID_A)), "peers never added"
    print("ok: both sides added each other")

    blob = os.urandom(5 * 1024 * 1024)
    assert peer_on_b.send({"type": "test_blob", "from": ID_B}, blob)
    assert got_blob.wait(10), "payload never arrived"
    assert received["payload"] == blob, "payload corrupted"
    print("ok: 5 MB payload arrived intact")

    peer_on_b.disconnect()
    assert wait_for(lambda: not table_a.has(ID_B)), "A never noticed B leaving"
    assert wait_for(lambda: not table_b.has(ID_A)), "B never cleaned up"
    print("ok: disconnect detected, peer removed on both sides")
    listener.close()


def test_simultaneous_dial():
    connection.register_handler("file_list", lambda peer, msg, payload: None)
    table_a, table_b = PeerTable(ID_A), PeerTable(ID_B)
    listen_a = socket.create_server(("127.0.0.1", 0))
    listen_b = socket.create_server(("127.0.0.1", 0))

    a_out = socket.create_connection(listen_b.getsockname())
    b_out = socket.create_connection(listen_a.getsockname())
    b_in, b_in_addr = listen_b.accept()
    a_in, a_in_addr = listen_a.accept()
    connection.start_peer_thread(a_out, a_out.getpeername(), table_a, True, hello(ID_A))
    connection.start_peer_thread(b_out, b_out.getpeername(), table_b, True, hello(ID_B))
    connection.start_peer_thread(b_in, b_in_addr, table_b, False, hello(ID_B))
    connection.start_peer_thread(a_in, a_in_addr, table_a, False, hello(ID_A))

    time.sleep(1.0)
    peer_a, peer_b = table_a.get(ID_B), table_b.get(ID_A)
    assert peer_a and peer_b, "a node lost its peer entirely"
    same = peer_a.sock.getsockname() == peer_b.sock.getpeername()
    assert same, "the two nodes kept different connections"
    print("ok: simultaneous dial left exactly one shared connection")
    listen_a.close()
    listen_b.close()


if __name__ == "__main__":
    test_connect_transfer_disconnect()
    test_simultaneous_dial()
    print("all tests passed")
