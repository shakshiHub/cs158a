# socket for shutting down connections, threading for locks
import socket
import threading

# logging and address formatting, plus the message sender
from logger import fmt_addr, log_event
from protocol import send_msg


# the first 8 characters of an id, or "?" if there's no id yet
def short(node_id):
    return node_id[:8] if node_id else "?"


# one connection to another node
class Peer:

    # everything known about this connection
    def __init__(self, sock, addr, outgoing):
        # the TCP socket and the (ip, port) on the other end
        self.sock = sock
        self.addr = addr

        # True if this node connected to them, False if they connected to this node
        self.outgoing = outgoing

        # their id, unknown until their first message arrives
        self.node_id = None

        # the files they advertise, from their latest file list
        self.files = []

        # only one thread at a time can send on this socket
        self.send_lock = threading.Lock()

    # a message sent to this peer; True if it worked, False if not
    def send(self, msg, payload=b""):
        try:
            # the lock keeps two sends from mixing their bytes together
            with self.send_lock:
                send_msg(self.sock, msg, payload, self.addr)
            return True

        # a failed send means the connection is broken, so it gets closed
        except OSError:
            self.disconnect()
            return False

    # the connection shut down, which wakes up and ends this peer's receive loop
    def disconnect(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)

        # already closed, so there's nothing to do
        except OSError:
            pass


# every connected peer, looked up by node id
class PeerTable:

    # an empty table for this node
    def __init__(self, my_id):
        # this node's own id, used to settle duplicate connections
        self.my_id = my_id

        # node id -> Peer
        self._peers = {}

        # many threads use this table, so only one can change it at a time
        self._lock = threading.Lock()

    # a new peer added; returns True if this connection is the one kept
    def add(self, peer):
        # the old connection that gets replaced, if any
        displaced = None

        with self._lock:
            # any connection already in the table for this node
            existing = self._peers.get(peer.node_id)

            # a second connection to the same node: only one can stay.
            # both nodes keep the one started by the smaller id,
            # so they always agree on which one
            if existing is not None:
                # the old connection wins, so the new one isn't kept
                if self._dialer(peer) >= self._dialer(existing):
                    keep_new = False

                # the new connection wins and takes the old one's place
                else:
                    keep_new = True
                    displaced = existing
                    self._peers[peer.node_id] = peer

            # a brand new peer goes straight into the table
            else:
                keep_new = True
                self._peers[peer.node_id] = peer

            # the number of peers now, for the log line
            count = len(self._peers)

        # the replaced connection gets closed (outside the lock, since it can be slow)
        if displaced is not None:
            displaced.disconnect()
            log_event(f"duplicate connection to {short(peer.node_id)}: "
                      f"replaced {fmt_addr(displaced.addr)} with {fmt_addr(peer.addr)}")

        # the new connection lost, so its receive loop closes it
        elif not keep_new:
            log_event(f"duplicate connection to {short(peer.node_id)} at "
                      f"{fmt_addr(peer.addr)}: closing it")

        # a normal new peer
        else:
            log_event(f"peer added: {short(peer.node_id)} at {fmt_addr(peer.addr)} "
                      f"({count} peer{'s' if count != 1 else ''})")

        return keep_new

    # a peer and its files taken out of the table after it leaves
    def remove(self, peer):
        with self._lock:
            # only this exact connection is removed, so closing a
            # duplicate never removes the good connection
            removed = self._peers.get(peer.node_id) is peer
            if removed:
                del self._peers[peer.node_id]

            # the number of peers left, for the log line
            count = len(self._peers)

        # a log line only when something was really removed
        if removed:
            log_event(f"peer removed: {short(peer.node_id)} at {fmt_addr(peer.addr)}, "
                      f"its {len(peer.files)} file(s) dropped "
                      f"({count} peer{'s' if count != 1 else ''} left)")
        return removed

    # a peer's file list replaced with the newest one
    def set_files(self, peer, files):
        with self._lock:
            peer.files = list(files)

    # True if this node is connected to that node id
    def has(self, node_id):
        with self._lock:
            return node_id in self._peers

    # the Peer for that node id, or None
    def get(self, node_id):
        with self._lock:
            return self._peers.get(node_id)

    # a copy of the list of peers, safe to loop over without the lock
    def all_peers(self):
        with self._lock:
            return list(self._peers.values())

    # a copy of the whole table, for printing: {id: {"addr", "files"}}
    def snapshot(self):
        with self._lock:
            return {nid: {"addr": p.addr, "files": list(p.files)}
                    for nid, p in self._peers.items()}

    # a copy of one peer's advertised names, or None if that peer is gone
    def files_of(self, node_id):
        with self._lock:
            peer = self._peers.get(node_id)
            if peer is None:
                return None
            return list(peer.files)

    # the ids of every peer that has this file
    def who_has(self, filename):
        with self._lock:
            return [nid for nid, p in self._peers.items() if filename in p.files]

    # the id of whichever node started this connection
    def _dialer(self, peer):
        return self.my_id if peer.outgoing else peer.node_id
