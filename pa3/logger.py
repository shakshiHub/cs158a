import threading
from datetime import datetime

LOG_FILE = "log.txt"

_lock = threading.Lock()


def fmt_addr(addr):
    if addr is None:
        return "-"
    if isinstance(addr, tuple) and len(addr) >= 2:
        return f"{addr[0]}:{addr[1]}"
    return str(addr)


def _write(line, echo=False):
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    entry = f"{stamp} | {line}"
    with _lock:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(entry + "\n")
        if echo:
            print(entry, flush=True)


def log_message(direction, peer_addr, msg_type):
    _write(f"{direction.upper():<8} | {fmt_addr(peer_addr):<21} | {msg_type}")


def log_event(text, echo=True):
    _write(f"{'EVENT':<8} | {'-':<21} | {text}", echo=echo)
