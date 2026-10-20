import socket
import threading

from logger import fmt_addr, log_event
from protocol import send_msg


def short(node_id):
    return node_id[:8] if node_id else "?"


class Peer:

    def __init__(self, sock, addr, outgoing):
        self.sock = sock
        self.addr = addr
        self.outgoing = outgoing
        self.node_id = None
        self.files = []
        self.send_lock = threading.Lock()

    def send(self, msg, payload=b""):
        try:
            with self.send_lock:
                send_msg(self.sock, msg, payload, self.addr)
            return True
        except OSError:
            self.disconnect()
            return False

    def disconnect(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


class PeerTable:

    def __init__(self, my_id):
        self.my_id = my_id
        self._peers = {}
        self._lock = threading.Lock()

    def add(self, peer):
        displaced = None
        with self._lock:
            existing = self._peers.get(peer.node_id)
            if existing is not None:
                if self._dialer(peer) >= self._dialer(existing):
                    keep_new = False
                else:
                    keep_new = True
                    displaced = existing
                    self._peers[peer.node_id] = peer
            else:
                keep_new = True
                self._peers[peer.node_id] = peer
            count = len(self._peers)

        if displaced is not None:
            displaced.disconnect()
            log_event(f"duplicate connection to {short(peer.node_id)}: "
                      f"replaced {fmt_addr(displaced.addr)} with {fmt_addr(peer.addr)}")
        elif not keep_new:
            log_event(f"duplicate connection to {short(peer.node_id)} at "
                      f"{fmt_addr(peer.addr)}: closing it")
        else:
            log_event(f"peer added: {short(peer.node_id)} at {fmt_addr(peer.addr)} "
                      f"({count} peer{'s' if count != 1 else ''})")
        return keep_new

    def remove(self, peer):
        with self._lock:
            removed = self._peers.get(peer.node_id) is peer
            if removed:
                del self._peers[peer.node_id]
            count = len(self._peers)
        if removed:
            log_event(f"peer removed: {short(peer.node_id)} at {fmt_addr(peer.addr)}, "
                      f"its {len(peer.files)} file(s) dropped ({count} peers left)")
        return removed

    def set_files(self, peer, files):
        with self._lock:
            peer.files = list(files)

    def has(self, node_id):
        with self._lock:
            return node_id in self._peers

    def get(self, node_id):
        with self._lock:
            return self._peers.get(node_id)

    def all_peers(self):
        with self._lock:
            return list(self._peers.values())

    def snapshot(self):
        with self._lock:
            return {nid: {"addr": p.addr, "files": list(p.files)}
                    for nid, p in self._peers.items()}

    def who_has(self, filename):
        with self._lock:
            return [nid for nid, p in self._peers.items() if filename in p.files]

    def _dialer(self, peer):
        return self.my_id if peer.outgoing else peer.node_id
