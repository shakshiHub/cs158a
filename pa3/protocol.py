# json for the header, struct for the 4-byte length
import json
import struct

# logging for every sent and received message
from logger import log_message

# "!I" means a 4-byte unsigned number in network byte order
LENGTH_FORMAT = "!I"

# the size of that number in bytes (always 4)
LENGTH_SIZE = struct.calcsize(LENGTH_FORMAT)

# a header bigger than 64 KB means the message is broken
MAX_HEADER_SIZE = 64 * 1024

# the biggest file allowed in one message (1 GB)
MAX_PAYLOAD_SIZE = 1024 * 1024 * 1024

# this node's id, set once at startup
_node_id = None


# error for a broken message from a peer
class ProtocolError(Exception):
    pass


# saves this node's id for the "from" field
def set_node_id(node_id):
    # the module-level variable, not a local one
    global _node_id
    _node_id = node_id


# exactly n bytes from the socket, or an error if the peer leaves partway
def recv_exact(sock, n):
    # the pieces read so far, and how many bytes are still missing
    chunks = []
    remaining = n

    # one recv() can return less than asked, so the loop runs until nothing is missing
    while remaining > 0:
        # at most 64 KB per read, and never more than what's still needed
        chunk = sock.recv(min(remaining, 65536))

        # an empty read means the peer closed the connection
        if not chunk:
            raise ConnectionError("peer closed the connection")

        # the new piece is kept and the missing count goes down
        chunks.append(chunk)
        remaining -= len(chunk)

    # all the pieces joined into one bytes object
    return b"".join(chunks)


# one message out: length, header, file bytes, then a log line
def send_msg(sock, msg, payload=b"", peer_addr=None):
    # a copy of the message, so the caller's dict stays the same
    header = dict(msg)

    # this node's id, unless the message already has a "from"
    header.setdefault("from", _node_id)

    # how many file bytes come after the header
    header["payload_len"] = len(payload)

    # the header as JSON text, turned into bytes
    header_bytes = json.dumps(header).encode("utf-8")

    # the 4-byte length followed by the header, in one send
    sock.sendall(struct.pack(LENGTH_FORMAT, len(header_bytes)) + header_bytes)

    # the raw file bytes, only if there are any
    if payload:
        sock.sendall(payload)

    # a log line for the sent message
    log_message("sent", peer_addr, header["type"])


# one message in: length, header, file bytes, with a check at each step
def recv_msg(sock, peer_addr=None):
    # step 1: the 4-byte header length
    (header_len,) = struct.unpack(LENGTH_FORMAT, recv_exact(sock, LENGTH_SIZE))

    # a length of 0 or over the limit means the stream is broken
    if header_len == 0 or header_len > MAX_HEADER_SIZE:
        raise ProtocolError(f"bad header length {header_len}")

    # step 2: the header bytes, turned from JSON into a dict
    try:
        header = json.loads(recv_exact(sock, header_len).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ProtocolError(f"header is not valid JSON: {e}") from e

    # every header has to be a dict with a "type"
    if not isinstance(header, dict) or not isinstance(header.get("type"), str):
        raise ProtocolError("header has no message type")

    # the number of file bytes, which has to be a sensible whole number
    payload_len = header.get("payload_len", 0)
    if not isinstance(payload_len, int) or not 0 <= payload_len <= MAX_PAYLOAD_SIZE:
        raise ProtocolError(f"bad payload length {payload_len!r}")

    # step 3: the file bytes, or empty bytes if there are none
    payload = recv_exact(sock, payload_len) if payload_len else b""

    # a log line for the received message
    log_message("received", peer_addr, header["type"])

    # the header dict and the file bytes, together
    return header, payload
