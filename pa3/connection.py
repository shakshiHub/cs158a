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
def start_peer_thread(sock, addr, peer_table, outgoing, on_connect=None, on_finish=None):
    # the Peer object that wraps the socket
    peer = Peer(sock, addr, outgoing)

    # which table this connection belongs to. two nodes tested in one
    # process each have their own table, so this cannot be a global.
    peer.table = peer_table

    # the reading thread; daemon=True means it stops when the program exits
    threading.Thread(target=_peer_loop,
                     args=(peer, peer_table, on_connect, on_finish),
                     name=f"peer-{fmt_addr(addr)}",
                     daemon=True).start()

    # the Peer goes back to the caller (its id is unknown until the first message)
    return peer


# the reading loop for one peer, running until that peer leaves
def _peer_loop(peer, peer_table, on_connect, on_finish=None):
    # whether this peer made it into the table, so cleanup knows to remove it
    in_table = False

    try:
        # our file list goes out first, right after connecting
        if on_connect is not None:
            on_connect(peer)

        # the first message from the peer
        msg, payload = recv_msg(peer.sock, peer.addr)

        # the dialer may have set a short timeout so a peer that never speaks
        # does not occupy a connection forever. later reads wait until the peer
        # sends something or the socket closes.
        if hasattr(peer.sock, "settimeout"):
            try:
                peer.sock.settimeout(None)
            except OSError:
                pass

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

    # a broken message from the peer, which ends this connection only
    except ProtocolError as e:
        log_event(f"protocol error from {fmt_addr(peer.addr)}: {e}; closing connection")

    # the OS reports a reset. this is a subclass of ConnectionError, so it
    # has to be caught first. the node keeps running; only this peer is dropped.
    except ConnectionResetError:
        log_event(f"connection reset by {short(peer.node_id)} at {fmt_addr(peer.addr)}",
                  echo=False)

    # recv_exact raises this when recv() returns b"" (a clean close)
    except ConnectionError:
        log_event(f"peer closed the connection: {short(peer.node_id)} at {fmt_addr(peer.addr)}",
                  echo=False)

    # any other socket failure on this connection
    except OSError as e:
        log_event(f"socket error from {fmt_addr(peer.addr)}: {e}", echo=False)

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

        # lets the dialer forget an in-progress connection
        if on_finish is not None:
            try:
                on_finish(peer)
            except Exception as e:
                log_event(f"on_finish failed: {e!r}", echo=False)


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
