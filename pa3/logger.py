# threading for the lock, datetime for timestamps
import threading
from datetime import datetime

# the log file, in the folder the node is started from
LOG_FILE = "log.txt"

# many threads log at once, so only one can write at a time
_lock = threading.Lock()


# an address as "ip:port" text
def fmt_addr(addr):
    # no address at all shows up as "-"
    if addr is None:
        return "-"

    # an (ip, port) tuple becomes "ip:port"
    if isinstance(addr, tuple) and len(addr) >= 2:
        return f"{addr[0]}:{addr[1]}"

    # anything else is shown as plain text
    return str(addr)


# one timestamped line added to the log file, and printed too if echo is True
def _write(line, echo=False):
    # seconds only: the assignment's sample lines do not include milliseconds
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # the full log line: time, then the message
    entry = f"{stamp} {line}"

    # the lock keeps lines from different threads from mixing together
    with _lock:
        # "a" means the line is added to the end of the file
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(entry + "\n")

        # the same line on screen, for events during the demo
        if echo:
            print(entry, flush=True)


# a log line for a message: direction, type, and the peer address or node id
# file bytes are never written here
def log_message(direction, peer_addr, msg_type):
    direction = direction.upper()
    # "to" for outgoing traffic, "from" for incoming traffic
    if direction == "SENT":
        line = f"SENT {msg_type} to {fmt_addr(peer_addr)}"
    elif direction == "RECEIVED":
        line = f"RECEIVED {msg_type} from {fmt_addr(peer_addr)}"
    else:
        line = f"{direction} {msg_type} {fmt_addr(peer_addr)}"
    _write(line)


# a log line for an event, like a peer being added or removed (printed on screen by default)
def log_event(text, echo=True):
    _write(f"EVENT {text}", echo=echo)
