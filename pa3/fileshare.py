"""Local shared files, file-list messages, and file transfers.

File bytes travel in the raw payload that follows the JSON header.
protocol.py caps that header at 64 KB and already carries a separate
payload, so the bytes are not Base64 and they are not placed inside the JSON.
"""

import os
import shutil
import threading

from logger import log_event
from peers import short
from protocol import MAX_PAYLOAD_SIZE

# filled in by configure_dirs before any listing or transfer
SHARED_DIR = "shared"
DOWNLOADS_DIR = "downloads"

# one transfer at a time, so two receives cannot write the same name at once
_fs_lock = threading.Lock()


# point this node at its own shared/ and downloads/ folders
def configure_dirs(shared_dir, downloads_dir):
    global SHARED_DIR, DOWNLOADS_DIR
    SHARED_DIR = shared_dir
    DOWNLOADS_DIR = downloads_dir
    os.makedirs(SHARED_DIR, exist_ok=True)
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)


# True only for a single path component. Rejects ../secret.txt and absolute paths.
def is_safe_filename(name):
    if not isinstance(name, str) or name in ("", ".", ".."):
        return False
    # basename() changes as soon as a directory separator is present
    if os.path.isabs(name) or name != os.path.basename(name):
        return False
    if "/" in name or "\\" in name or "\x00" in name:
        return False
    # a name our own save path uses for a half-written copy
    if name.startswith(".") and name.endswith(".part"):
        return False
    if len(name.encode("utf-8")) > 255:
        return False
    return True


# the full path of a name inside shared/, or None if the name is not safe
def safe_shared_path(filename):
    if not is_safe_filename(filename):
        return None
    root = os.path.realpath(SHARED_DIR)
    candidate = os.path.realpath(os.path.join(root, filename))
    try:
        inside = os.path.commonpath([root, candidate]) == root
    except ValueError:
        return None
    if not inside:
        return None
    return candidate


# True when shared/ already has this file. Callers must not request or overwrite it.
def have_shared(filename):
    path = safe_shared_path(filename)
    return path is not None and os.path.isfile(path)


# filenames of regular files in shared/. Directories are not offered.
def list_shared_files():
    if not os.path.isdir(SHARED_DIR):
        return []
    names = []
    for entry in os.scandir(SHARED_DIR):
        # follow_symlinks=False so a directory link is not advertised as a file
        if not entry.is_file(follow_symlinks=False):
            continue
        if is_safe_filename(entry.name):
            names.append(entry.name)
    names.sort()
    return names


# the file_list message for whatever is in shared/ right now
def file_list_message():
    return {"type": "file_list", "files": list_shared_files()}


# one peer gets our current list. payload is empty; names are in the header.
def send_file_list(peer):
    return peer.send(file_list_message())


# every connected peer gets the same list
def broadcast_file_list(peer_table):
    message = file_list_message()
    for peer in peer_table.all_peers():
        peer.send(message)


# wire the three file messages to the connection dispatcher
def register(connection_module):
    connection_module.register_handler("file_list", handle_file_list)
    connection_module.register_handler("data_request", handle_data_request)
    connection_module.register_handler("file_data", handle_file_data)


# replace this peer's advertised names. unsafe names are dropped, not stored.
def handle_file_list(peer, msg, payload):
    raw = msg.get("files")
    if not isinstance(raw, list):
        log_event(f"ignored file_list from {short(peer.node_id)}: files is not a list",
                  echo=False)
        return

    clean = []
    for name in raw:
        if is_safe_filename(name) and name not in clean:
            clean.append(name)

    table = getattr(peer, "table", None)
    if table is None or not peer.node_id:
        return

    previous = table.files_of(peer.node_id)
    table.set_files(peer, clean)
    # the 10-second repeat would flood the terminal, so only changes are printed
    if previous != clean:
        shown = ", ".join(clean) if clean else "(none)"
        log_event(f"file list from {short(peer.node_id)}: {shown}")


# a peer asked us for a file. an error reply leaves the TCP connection open.
def handle_data_request(peer, msg, payload):
    filename = msg.get("filename")
    if not is_safe_filename(filename):
        _send_file_error(peer, filename, "Invalid filename")
        return

    path = safe_shared_path(filename)
    if path is None or not os.path.isfile(path):
        _send_file_error(peer, filename, "File not found")
        return

    try:
        with open(path, "rb") as handle:
            data = handle.read()
    except OSError as exc:
        log_event(f"could not read {filename}: {exc}", echo=False)
        _send_file_error(peer, filename, "File not found")
        return

    if len(data) > MAX_PAYLOAD_SIZE:
        _send_file_error(peer, filename, "File too large")
        return

    # the bytes go in the raw payload. the header only names the file.
    peer.send({
        "type": "file_data",
        "filename": filename,
        "success": True,
    }, data)


# store a completed file_data message, or record the error and keep going
def handle_file_data(peer, msg, payload):
    filename = msg.get("filename")
    if msg.get("success") is not True:
        error = msg.get("error") or "transfer failed"
        log_event(f"file_data error for {filename!r} from {short(peer.node_id)}: {error}")
        return

    if not isinstance(payload, (bytes, bytearray)):
        log_event(f"file_data for {filename!r} had no byte payload")
        return

    copied = save_incoming_file(filename, bytes(payload))
    table = getattr(peer, "table", None)
    # the new name is offered on the next file_list, sent now and again every 10 seconds
    if copied and table is not None:
        broadcast_file_list(table)


# ask one peer for a file that peer has advertised and we do not already have
def request_file(peer_table, peer_id, filename):
    if not is_safe_filename(filename):
        return f"rejected: {filename!r} is not a plain filename"

    if have_shared(filename):
        return (f"rejected: {filename} is already in shared/; "
                "it will not be requested or overwritten")

    peer = peer_table.get(peer_id)
    if peer is None:
        known = ", ".join(sorted(item.node_id for item in peer_table.all_peers())) or "(none)"
        return f"rejected: no connected peer with id {peer_id}. connected: {known}"

    advertised = peer_table.files_of(peer_id)
    if advertised is None or filename not in advertised:
        return f"rejected: {peer_id} does not advertise {filename}"

    if not peer.send({"type": "data_request", "filename": filename}):
        return f"rejected: could not send data_request to {peer_id}"

    log_event(f"requested {filename} from {short(peer_id)}")
    return f"requested {filename} from {peer_id}"


# downloads/ first, and only then shared/. shared/ is left alone if the name exists.
def save_incoming_file(filename, data):
    if not is_safe_filename(filename):
        log_event(f"rejected file_data with unsafe name {filename!r}")
        return False

    with _fs_lock:
        os.makedirs(DOWNLOADS_DIR, exist_ok=True)
        os.makedirs(SHARED_DIR, exist_ok=True)
        partial = os.path.join(DOWNLOADS_DIR, f".{filename}.part")
        download_path = os.path.join(DOWNLOADS_DIR, filename)
        try:
            with open(partial, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            # the name appears in downloads/ only after the whole write finishes
            os.replace(partial, download_path)
        except OSError as exc:
            log_event(f"could not save {filename} to downloads/: {exc}")
            _remove_quiet(partial)
            return False

        shared_path = safe_shared_path(filename)
        if shared_path is None:
            return False
        if os.path.exists(shared_path):
            log_event(
                f"{filename} downloaded ({len(data)} bytes) but already in shared/; not overwritten"
            )
            return False

        shared_partial = os.path.join(SHARED_DIR, f".{filename}.part")
        try:
            # copy the finished download. a crash here leaves the .part name, which is not advertised.
            with open(download_path, "rb") as src, open(shared_partial, "wb") as dst:
                shutil.copyfileobj(src, dst)
                dst.flush()
                os.fsync(dst.fileno())
            os.replace(shared_partial, shared_path)
        except OSError as exc:
            log_event(f"could not copy {filename} into shared/: {exc}")
            _remove_quiet(shared_partial)
            return False

        log_event(f"saved {filename} ({len(data)} bytes) to downloads/ and shared/")
        return True


def _send_file_error(peer, filename, error):
    shown = filename if isinstance(filename, str) else ""
    peer.send({
        "type": "file_data",
        "filename": shown,
        "success": False,
        "error": error,
    })


def _remove_quiet(path):
    try:
        os.remove(path)
    except OSError:
        pass
