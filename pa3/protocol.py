import json
import struct

from logger import log_message

LENGTH_FORMAT = "!I"
LENGTH_SIZE = struct.calcsize(LENGTH_FORMAT)
MAX_HEADER_SIZE = 64 * 1024
MAX_PAYLOAD_SIZE = 1024 * 1024 * 1024

_node_id = None


class ProtocolError(Exception):
    pass


def set_node_id(node_id):
    global _node_id
    _node_id = node_id


def recv_exact(sock, n):
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(min(remaining, 65536))
        if not chunk:
            raise ConnectionError("peer closed the connection")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def send_msg(sock, msg, payload=b"", peer_addr=None):
    header = dict(msg)
    header.setdefault("from", _node_id)
    header["payload_len"] = len(payload)
    header_bytes = json.dumps(header).encode("utf-8")

    sock.sendall(struct.pack(LENGTH_FORMAT, len(header_bytes)) + header_bytes)
    if payload:
        sock.sendall(payload)

    log_message("sent", peer_addr, header["type"])


def recv_msg(sock, peer_addr=None):
    (header_len,) = struct.unpack(LENGTH_FORMAT, recv_exact(sock, LENGTH_SIZE))
    if header_len == 0 or header_len > MAX_HEADER_SIZE:
        raise ProtocolError(f"bad header length {header_len}")

    try:
        header = json.loads(recv_exact(sock, header_len).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ProtocolError(f"header is not valid JSON: {e}") from e

    if not isinstance(header, dict) or not isinstance(header.get("type"), str):
        raise ProtocolError("header has no message type")

    payload_len = header.get("payload_len", 0)
    if not isinstance(payload_len, int) or not 0 <= payload_len <= MAX_PAYLOAD_SIZE:
        raise ProtocolError(f"bad payload length {payload_len!r}")

    payload = recv_exact(sock, payload_len) if payload_len else b""

    log_message("received", peer_addr, header["type"])
    return header, payload
