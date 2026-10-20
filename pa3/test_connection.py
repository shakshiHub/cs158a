import os
import socket
import threading
import time

# our own modules
import connection
import logger
from peers import PeerTable

# test logs go to a separate file
logger.LOG_FILE = "test_log.txt"

# fake ids for the two test nodes
ID_A = "aaaaaaaa-0000-0000-0000-000000000000"
ID_B = "bbbbbbbb-0000-0000-0000-000000000000"


# sends an empty file list as the first message, like a real node
def hello(my_id):
    return lambda peer: peer.send({"type": "file_list", "from": my_id, "files": []})


# checks the condition every 0.02 seconds until it's true or time runs out
def wait_for(condition, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False


# B connects to A's listener, and both sides start a reading thread
def connect_pair(listener, table_a, table_b):
    b_sock = socket.create_connection(listener.getsockname())
    a_sock, a_addr = listener.accept()
    peer_on_a = connection.start_peer_thread(a_sock, a_addr, table_a, False, hello(ID_A))
    peer_on_b = connection.start_peer_thread(b_sock, b_sock.getpeername(), table_b, True, hello(ID_B))
    return peer_on_a, peer_on_b


# test 1: connect, send a big file, then disconnect
def test_connect_transfer_disconnect():
    # where the received file gets stored, and a flag for when it arrives
    received = {}
    got_blob = threading.Event()

    # handler that saves the file and sets the flag
    def on_blob(peer, msg, payload):
        received["payload"] = payload
        got_blob.set()

    # file lists are ignored here; test_blob goes to on_blob
    connection.register_handler("file_list", lambda peer, msg, payload: None)
    connection.register_handler("test_blob", on_blob)

    # two peer tables, one listener, and a connection between A and B
    table_a, table_b = PeerTable(ID_A), PeerTable(ID_B)
    listener = socket.create_server(("127.0.0.1", 0))
    _, peer_on_b = connect_pair(listener, table_a, table_b)

    # both nodes should have each other in their tables
    assert wait_for(lambda: table_a.has(ID_B) and table_b.has(ID_A)), "peers never added"
    print("ok: both sides added each other")

    # B sends 5 MB of random bytes, and A should get the exact same bytes
    blob = os.urandom(5 * 1024 * 1024)
    assert peer_on_b.send({"type": "test_blob", "from": ID_B}, blob)
    assert got_blob.wait(10), "payload never arrived"
    assert received["payload"] == blob, "payload corrupted"
    print("ok: 5 MB payload arrived intact")

    # B leaves, and both tables should end up empty
    peer_on_b.disconnect()
    assert wait_for(lambda: not table_a.has(ID_B)), "A never noticed B leaving"
    assert wait_for(lambda: not table_b.has(ID_A)), "B never cleaned up"
    print("ok: disconnect detected, peer removed on both sides")
    listener.close()


# test 2: A and B connect to each other at the same time
def test_simultaneous_dial():
    # file lists are ignored, each node gets a table and a listener
    connection.register_handler("file_list", lambda peer, msg, payload: None)
    table_a, table_b = PeerTable(ID_A), PeerTable(ID_B)
    listen_a = socket.create_server(("127.0.0.1", 0))
    listen_b = socket.create_server(("127.0.0.1", 0))

    # A connects to B, and B connects to A, so there are two connections
    a_out = socket.create_connection(listen_b.getsockname())
    b_out = socket.create_connection(listen_a.getsockname())
    b_in, b_in_addr = listen_b.accept()
    a_in, a_in_addr = listen_a.accept()

    # a reading thread for every socket on both sides
    connection.start_peer_thread(a_out, a_out.getpeername(), table_a, True, hello(ID_A))
    connection.start_peer_thread(b_out, b_out.getpeername(), table_b, True, hello(ID_B))
    connection.start_peer_thread(b_in, b_in_addr, table_b, False, hello(ID_B))
    connection.start_peer_thread(a_in, a_in_addr, table_a, False, hello(ID_A))

    # after a second, each node should still have the other in its table
    time.sleep(1.0)
    peer_a, peer_b = table_a.get(ID_B), table_b.get(ID_A)
    assert peer_a and peer_b, "a node lost its peer entirely"

    # both nodes should have kept the same connection
    same = peer_a.sock.getsockname() == peer_b.sock.getpeername()
    assert same, "the two nodes kept different connections"
    print("ok: simultaneous dial left exactly one shared connection")
    listen_a.close()
    listen_b.close()


# runs both tests
if __name__ == "__main__":
    test_connect_transfer_disconnect()
    test_simultaneous_dial()
    print("all tests passed")
