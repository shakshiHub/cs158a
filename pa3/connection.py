# threading for one reading thread per peer
import threading

# logging, the Peer class, and the message reader
from logger import fmt_addr, log_event
from peers import Peer, short
from protocol import ProtocolError, recv_msg

# message type -> the function that handles it, filled in by the file-handling code
HANDLERS = {}


# the function that gets called whenever this message type arrives
def register_handler(msg_type, func):
    HANDLERS[msg_type] = func


# a new Peer for this socket, with its own reading thread started
def start_peer_thread(sock, addr, peer_table, outgoing, on_connect=None):
    # the Peer object that wraps the socket
    peer = Peer(sock, addr, outgoing)

    # the reading thread; daemon=True means it stops when the program exits
    threading.Thread(target=_peer_loop,
                     args=(peer, peer_table, on_connect),
                     name=f"peer-{fmt_addr(addr)}",
                     daemon=True).start()

    # the Peer goes back to the caller (its id is unknown until the first message)
    return peer


# the reading loop for one peer, running until that peer leaves
def _peer_loop(peer, peer_table, on_connect):
    # whether this peer made it into the table, so cleanup knows to remove it
    in_table = False

    try:
        # our file list goes out first, right after connecting
        if on_connect is not None:
            on_connect(peer)

        # the first message from the peer
        msg, payload = recv_msg(peer.sock, peer.addr)

        # its "from" field says who the peer is; a missing id means a broken message
        sender = msg.get("from")
        if not isinstance(sender, str) or not sender:
            raise ProtocolError("first message has no sender id")
        peer.node_id = sender

        # the peer added to the table, unless it's a duplicate connection
        in_table = peer_table.add(peer)
        if not in_table:
            return

        # the first message still gets handled like any other
        _dispatch(peer, msg, payload)

        # every message after that, read and handled one at a time
        while True:
            msg, payload = recv_msg(peer.sock, peer.addr)
            _dispatch(peer, msg, payload)

    # a broken message from the peer, which ends the connection
    except ProtocolError as e:
        log_event(f"protocol error from {fmt_addr(peer.addr)}: {e}; closing connection")

    # the peer closed the connection or it was reset, so the peer is gone
    except OSError:
        pass

    # cleanup that always happens when the loop ends
    finally:
        # the peer and its files taken out of the table
        if in_table:
            peer_table.remove(peer)

        # the socket closed (it may already be closed)
        try:
            peer.sock.close()
        except OSError:
            pass


# a message passed to the function registered for its type
def _dispatch(peer, msg, payload):
    # the handler for this message type
    handler = HANDLERS.get(msg["type"])

    # no handler means the message is logged and ignored
    if handler is None:
        log_event(f"no handler for '{msg['type']}' from {short(peer.node_id)}; ignored")
        return

    # the handler runs; if it crashes, the error is logged and the loop keeps going
    try:
        handler(peer, msg, payload)
    except Exception as e:
        log_event(f"handler for '{msg['type']}' failed: {e!r}")
