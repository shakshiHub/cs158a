import threading

from logger import fmt_addr, log_event
from peers import Peer, short
from protocol import ProtocolError, recv_msg

HANDLERS = {}


def register_handler(msg_type, func):
    HANDLERS[msg_type] = func


def start_peer_thread(sock, addr, peer_table, outgoing, on_connect=None):
    peer = Peer(sock, addr, outgoing)
    threading.Thread(target=_peer_loop,
                     args=(peer, peer_table, on_connect),
                     name=f"peer-{fmt_addr(addr)}",
                     daemon=True).start()
    return peer


def _peer_loop(peer, peer_table, on_connect):
    in_table = False
    try:
        if on_connect is not None:
            on_connect(peer)

        msg, payload = recv_msg(peer.sock, peer.addr)
        sender = msg.get("from")
        if not isinstance(sender, str) or not sender:
            raise ProtocolError("first message has no sender id")
        peer.node_id = sender

        in_table = peer_table.add(peer)
        if not in_table:
            return
        _dispatch(peer, msg, payload)

        while True:
            msg, payload = recv_msg(peer.sock, peer.addr)
            _dispatch(peer, msg, payload)

    except ProtocolError as e:
        log_event(f"protocol error from {fmt_addr(peer.addr)}: {e}; closing connection")
    except OSError:
        pass
    finally:
        if in_table:
            peer_table.remove(peer)
        try:
            peer.sock.close()
        except OSError:
            pass


def _dispatch(peer, msg, payload):
    handler = HANDLERS.get(msg["type"])
    if handler is None:
        log_event(f"no handler for '{msg['type']}' from {short(peer.node_id)}; ignored")
        return
    try:
        handler(peer, msg, payload)
    except Exception as e:
        log_event(f"handler for '{msg['type']}' failed: {e!r}")
