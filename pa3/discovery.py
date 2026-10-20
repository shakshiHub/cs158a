"""UDP peer discovery.

UDP is used only to find peers. Every file list and every file byte goes over TCP.
All nodes bind the same UDP port. On one computer that requires SO_REUSEPORT.
A unicast reply to that port would be handed to only one of the sockets, so
peer_ack is broadcast too, and each node keeps the message only when "to" is itself.
"""

import json
import socket
import threading

from logger import fmt_addr, log_event, log_message

UDP_PORT = 54321
BROADCAST = "255.255.255.255"
# discovery datagrams are one small JSON object. anything larger is ignored.
MAX_DATAGRAM = 8192


def peer_request_message(node_id):
    return {"type": "peer_request", "from": node_id}


def peer_ack_message(node_id, to_id, tcp_port):
    return {"type": "peer_ack", "from": node_id, "to": to_id, "tcp_port": tcp_port}


def parse_udp_message(data):
    try:
        message = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(message, dict):
        return None
    return message


# what this node should do with one discovery message
def interpret_udp(message, my_id):
    if not isinstance(message, dict) or not isinstance(message.get("type"), str):
        return "malformed"
    sender = message.get("from")
    if not isinstance(sender, str) or not sender or sender == my_id:
        return "ignore"
    if message["type"] == "peer_request":
        return "ack"
    if message["type"] == "peer_ack":
        if message.get("to") != my_id:
            return "ignore"
        port = message.get("tcp_port")
        # bool is a subclass of int, and it is not a port
        if isinstance(port, bool) or not isinstance(port, int) or not 0 < port < 65536:
            return "ignore"
        return "connect"
    return "ignore"


def create_udp_socket(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # several nodes on one machine all bind UDP 54321. broadcast is delivered
    # to every one of them; unicast would be delivered to only one.
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind(("", port))
    return sock


class Discovery:

    def __init__(self, node_id, tcp_port, peer_table, dial, udp_port=UDP_PORT):
        self.node_id = node_id
        self.tcp_port = tcp_port
        self.peer_table = peer_table
        # dial(ip, tcp_port, remote_id, done) opens the TCP connection
        self.dial = dial
        self.udp_port = udp_port
        self.sock = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        # node ids we are already opening a TCP socket to
        self._dialing = set()

    def start(self):
        self.sock = create_udp_socket(self.udp_port)
        threading.Thread(target=self._listen, name="udp-listen", daemon=True).start()
        threading.Thread(target=self._announce_loop, name="udp-announce", daemon=True).start()

    def stop(self):
        self._stop.set()
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass

    # UDP can drop the first request, so it is repeated. already-connected
    # peers ignore the ACK that comes back.
    def _announce_loop(self):
        while not self._stop.is_set():
            self._send_request()
            if self._stop.wait(10):
                return

    def _send_request(self):
        self._broadcast(peer_request_message(self.node_id))

    def _broadcast(self, message):
        data = json.dumps(message).encode("utf-8")
        try:
            self.sock.sendto(data, (BROADCAST, self.udp_port))
        except OSError as exc:
            log_event(f"UDP send failed: {exc}", echo=False)
            return
        log_message("sent", (BROADCAST, self.udp_port), message["type"])

    def _listen(self):
        while not self._stop.is_set():
            try:
                data, addr = self.sock.recvfrom(65535)
            except OSError:
                break
            self._handle(data, addr)

    def _handle(self, data, addr):
        if len(data) > MAX_DATAGRAM:
            log_event(f"ignored oversized UDP datagram from {fmt_addr(addr)}", echo=False)
            return
        message = parse_udp_message(data)
        if message is None:
            log_event(f"ignored malformed UDP message from {fmt_addr(addr)}", echo=False)
            return

        sender = message.get("from")
        # our own broadcast comes back on this socket. it was already logged as SENT.
        if isinstance(sender, str) and sender == self.node_id:
            return
        if isinstance(message.get("type"), str) and isinstance(sender, str) and sender:
            log_message("received", sender, message["type"])

        action = interpret_udp(message, self.node_id)
        if action == "ack":
            self._broadcast(peer_ack_message(self.node_id, sender, self.tcp_port))
        elif action == "connect":
            self._start_dial(addr[0], message["tcp_port"], sender)

    def _start_dial(self, ip, port, remote_id):
        if self.peer_table.has(remote_id):
            return
        with self._lock:
            if remote_id in self._dialing:
                return
            self._dialing.add(remote_id)
        self.dial(ip, port, remote_id, self._dial_finished)

    def _dial_finished(self, remote_id):
        with self._lock:
            self._dialing.discard(remote_id)
