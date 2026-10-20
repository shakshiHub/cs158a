"""P2P data-sharing node.

Threads:
  main            command loop
  udp-listen      discovery datagrams
  udp-announce    peer_request every 10 seconds
  tcp-accept      incoming peer connections
  file-list       our filenames, every 10 seconds
  peer-<addr>     one receive loop per connected peer

UDP is only for discovery. After that, every message is a length-prefixed
TCP frame from protocol.py.
"""

import argparse
import os
import socket
import sys
import threading
import time

import connection
import discovery
import fileshare
import logger
from logger import fmt_addr, log_event
from peers import PeerTable
from protocol import set_node_id

HELP = """
commands:
  peers                         show connected peers
  files                         show the files each peer is advertising
  request <peer_id> <filename>  download a file that peer has advertised
  help                          show this message
  quit                          disconnect and exit
""".strip()


def _seconds(env_name, default):
    # the assignment interval is 10 seconds. tests can widen it so a file
    # deleted after it was advertised is still requested before the next list.
    raw = os.environ.get(env_name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def format_peers(peer_table):
    snap = peer_table.snapshot()
    if not snap:
        return "no peers connected"
    lines = []
    for node_id in sorted(snap):
        info = snap[node_id]
        lines.append(f"{node_id} {fmt_addr(info['addr'])} files={len(info['files'])}")
    return "\n".join(lines)


def format_files(peer_table):
    local = ", ".join(fileshare.list_shared_files()) or "(empty)"
    lines = [f"local shared: {local}"]
    snap = peer_table.snapshot()
    if not snap:
        lines.append("no peers connected")
        return "\n".join(lines)
    for node_id in sorted(snap):
        info = snap[node_id]
        names = ", ".join(info["files"]) if info["files"] else "(none advertised yet)"
        lines.append(f"{node_id} {fmt_addr(info['addr'])}: {names}")
    return "\n".join(lines)


class Node:

    def __init__(self, node_id, tcp_port, workdir, udp_port):
        self.node_id = node_id
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.workdir = os.path.abspath(workdir)
        self.peer_table = PeerTable(node_id)
        self.stop = threading.Event()
        self.tcp_sock = None
        self.discovery = None

    def start(self):
        # each process has one id, stamped onto every TCP header as "from"
        set_node_id(self.node_id)
        logger.LOG_FILE = os.path.join(self.workdir, "log.txt")
        fileshare.configure_dirs(
            os.path.join(self.workdir, "shared"),
            os.path.join(self.workdir, "downloads"),
        )
        fileshare.register(connection)

        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_sock.bind(("", self.tcp_port))
        self.tcp_sock.listen()
        threading.Thread(target=self._accept_loop, name="tcp-accept", daemon=True).start()

        # TCP is listening before we announce, so an ACK can be dialed immediately
        self.discovery = discovery.Discovery(
            self.node_id,
            self.tcp_port,
            self.peer_table,
            self._dial,
            self.udp_port,
        )
        self.discovery.start()
        threading.Thread(target=self._file_list_loop, name="file-list", daemon=True).start()

        log_event(
            f"{self.node_id} started; TCP {self.tcp_port}; UDP {self.udp_port}; "
            f"sharing {os.path.join(self.workdir, 'shared')}"
        )
        print("ready", flush=True)
        print(HELP, flush=True)

    def serve(self):
        self.start()
        try:
            self._command_loop()
        finally:
            self.shutdown()

    def shutdown(self):
        if self.stop.is_set():
            return
        self.stop.set()
        log_event(f"{self.node_id} shutting down")
        # shutdown wakes each peer's receive loop so the other side sees a close
        for peer in self.peer_table.all_peers():
            peer.disconnect()
        if self.tcp_sock is not None:
            try:
                self.tcp_sock.close()
            except OSError:
                pass
        if self.discovery is not None:
            self.discovery.stop()
        # give the receive threads a moment to send their FIN
        time.sleep(0.2)

    def _accept_loop(self):
        # a short timeout lets shutdown close the socket without waiting forever
        self.tcp_sock.settimeout(0.5)
        while not self.stop.is_set():
            try:
                conn, addr = self.tcp_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            _set_nodelay(conn)
            # drop the socket if the peer never sends its hello
            conn.settimeout(10)
            log_event(f"incoming TCP connection from {fmt_addr(addr)}")
            connection.start_peer_thread(
                conn, addr, self.peer_table, False, fileshare.send_file_list
            )

    # called on the UDP thread. the actual connect runs elsewhere so one slow
    # peer cannot stall discovery.
    def _dial(self, ip, port, remote_id, done):
        threading.Thread(
            target=self._open_outgoing,
            args=(ip, port, remote_id, done),
            name=f"dial-{remote_id}",
            daemon=True,
        ).start()

    def _open_outgoing(self, ip, port, remote_id, done):
        if self.peer_table.has(remote_id):
            done(remote_id)
            return
        try:
            sock = socket.create_connection((ip, int(port)), timeout=5)
            # the first frame has to arrive promptly; connection.py clears this
            # after that frame so an idle peer is not dropped every 10 seconds
            sock.settimeout(10)
            _set_nodelay(sock)
        except OSError as exc:
            log_event(f"could not connect to {remote_id} at {ip}:{port}: {exc}", echo=False)
            done(remote_id)
            return
        log_event(f"outgoing TCP connection to {remote_id} at {ip}:{port}")
        # done() runs when this connection's receive thread ends, so a failed
        # dial can be retried and a live one is not opened twice
        connection.start_peer_thread(
            sock,
            sock.getpeername(),
            self.peer_table,
            True,
            fileshare.send_file_list,
            on_finish=lambda peer: done(remote_id),
        )

    def _file_list_loop(self):
        # the first list goes out from on_connect. this loop is the repeat.
        while not self.stop.wait(_seconds("PA3_FILE_LIST_INTERVAL", 10)):
            fileshare.broadcast_file_list(self.peer_table)

    def _command_loop(self):
        while not self.stop.is_set():
            try:
                line = input("> ")
            except (EOFError, KeyboardInterrupt):
                print(flush=True)
                return
            if not self._handle_line(line):
                return

    def _handle_line(self, line):
        parts = line.strip().split(maxsplit=2)
        if not parts:
            return True
        command = parts[0].lower()
        if command in ("quit", "exit"):
            return False
        if command == "help":
            print(HELP, flush=True)
        elif command == "peers":
            print(format_peers(self.peer_table), flush=True)
        elif command == "files":
            print(format_files(self.peer_table), flush=True)
        elif command == "request":
            if len(parts) != 3:
                print("usage: request <peer_id> <filename>", flush=True)
            else:
                print(fileshare.request_file(self.peer_table, parts[1], parts[2]), flush=True)
        else:
            print(f"unknown command {command!r}. type 'help'.", flush=True)
        return True


def _set_nodelay(sock):
    # file-list and request messages are small. without this, Nagle can hold them.
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except OSError:
        pass


def build_parser():
    parser = argparse.ArgumentParser(description="P2P data sharing agent")
    parser.add_argument("--port", type=int, required=True, help="TCP port for this node")
    parser.add_argument("--node-id", required=True, help="id of this node")
    parser.add_argument("--udp-port", type=int, default=discovery.UDP_PORT,
                        help="UDP discovery port (default 54321)")
    parser.add_argument("--workdir", default=".",
                        help="folder for shared/, downloads/, and log.txt")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if not 0 < args.port < 65536 or not 0 < args.udp_port < 65536:
        print("port numbers must be between 1 and 65535", file=sys.stderr)
        return 1
    if not args.node_id.strip():
        print("node id must not be empty", file=sys.stderr)
        return 1
    node = Node(args.node_id, args.port, args.workdir, args.udp_port)
    try:
        node.serve()
    except OSError as exc:
        print(f"could not start {args.node_id}: {exc}", file=sys.stderr)
        node.shutdown()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
