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
    # the current time, down to milliseconds
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    # the full log line: time, then the message
    entry = f"{stamp} | {line}"

    # the lock keeps lines from different threads from mixing together
    with _lock:
        # "a" means the line is added to the end of the file
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(entry + "\n")

        # the same line on screen, for events during the demo
        if echo:
            print(entry, flush=True)


# a log line for a message: sent or received, the peer's address, and the message type
def log_message(direction, peer_addr, msg_type):
    _write(f"{direction.upper():<8} | {fmt_addr(peer_addr):<21} | {msg_type}")


# a log line for an event, like a peer being added or removed (printed on screen by default)
def log_event(text, echo=True):
    _write(f"{'EVENT':<8} | {'-':<21} | {text}", echo=echo)
