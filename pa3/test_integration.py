"""Three nodes on one computer: discovery, file lists, transfer, disconnect."""

import os
import socket
import subprocess
import sys
import tempfile
import textwrap
import time

ROOT = os.path.dirname(os.path.abspath(__file__))


def free_tcp_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def free_udp_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def start_node(node_id, tcp_port, udp_port, workdir, files):
    shared = os.path.join(workdir, "shared")
    os.makedirs(shared, exist_ok=True)
    for name, text in files.items():
        with open(os.path.join(shared, name), "w", encoding="utf-8") as handle:
            handle.write(text)
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    # keep the advertised name stable while the missing-file request is in flight
    env["PA3_FILE_LIST_INTERVAL"] = "30"
    proc = subprocess.Popen(
        [
            sys.executable,
            os.path.join(ROOT, "agent.py"),
            "--port", str(tcp_port),
            "--node-id", node_id,
            "--udp-port", str(udp_port),
            "--workdir", workdir,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        cwd=ROOT,
    )
    return proc


def pump(proc, lines):
    for line in proc.stdout:
        lines.append(line)


def wait_until(predicate, timeout, detail):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(detail)


def send(proc, command):
    proc.stdin.write(command + "\n")
    proc.stdin.flush()


def stop(proc):
    if proc.poll() is not None:
        return
    try:
        send(proc, "quit")
    except (BrokenPipeError, OSError):
        pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def main():
    udp_port = free_udp_port()
    ports = {name: free_tcp_port() for name in ("node1", "node2", "node3")}
    root = tempfile.mkdtemp(prefix="pa3-three-")
    files = {
        "node1": {"alpha.txt": "from-node1\n", "common.txt": "original-common\n"},
        "node2": {
            "beta.txt": "from-node2\n",
            "gone.txt": "temporary\n",
            "common.txt": "other-common\n",
        },
        "node3": {"gamma.txt": "from-node3\n"},
    }
    procs = {}
    output = {name: [] for name in files}
    threads = []
    try:
        for name in ("node1", "node2", "node3"):
            workdir = os.path.join(root, name)
            os.makedirs(workdir, exist_ok=True)
            proc = start_node(name, ports[name], udp_port, workdir, files[name])
            procs[name] = proc
            thread = threading_pump(proc, output[name])
            threads.append(thread)
            time.sleep(0.2)

        for name in procs:
            wait_until(
                lambda name=name: any(line.strip() == "ready" for line in output[name]),
                8,
                f"{name} did not become ready\n{''.join(output[name])}",
            )
        # each node should learn the other two
        for name, peers in (("node1", ("node2", "node3")), ("node2", ("node1", "node3")), ("node3", ("node1", "node2"))):
            for peer in peers:
                wait_until(
                    lambda name=name, peer=peer: any(
                        f"file list from {peer}:" in line for line in output[name]
                    ),
                    20,
                    f"{name} never got a file list from {peer}\n{''.join(output[name])}",
                )

        send(procs["node1"], "files")
        wait_until(
            lambda: any(line.startswith("node2 ") and "beta.txt" in line for line in output["node1"]),
            5,
            "files command did not show node2's beta.txt\n" + "".join(output["node1"]),
        )

        send(procs["node1"], "request node2 common.txt")
        wait_until(
            lambda: any("already in shared" in line for line in output["node1"]),
            5,
            "duplicate filename was not rejected\n" + "".join(output["node1"]),
        )
        common = os.path.join(root, "node1", "shared", "common.txt")
        assert open(common, encoding="utf-8").read() == "original-common\n"

        send(procs["node1"], "request node2 beta.txt")
        beta_dl = os.path.join(root, "node1", "downloads", "beta.txt")
        beta_sh = os.path.join(root, "node1", "shared", "beta.txt")
        wait_until(
            lambda: os.path.isfile(beta_dl) and os.path.isfile(beta_sh),
            10,
            "beta.txt was not saved\n" + "".join(output["node1"]),
        )
        assert open(beta_dl, encoding="utf-8").read() == "from-node2\n"
        assert open(beta_sh, encoding="utf-8").read() == "from-node2\n"

        os.remove(os.path.join(root, "node2", "shared", "gone.txt"))
        send(procs["node1"], "request node2 gone.txt")
        wait_until(
            lambda: any("File not found" in line for line in output["node1"]),
            10,
            "missing-file error was not reported\n" + "".join(output["node1"]),
        )
        assert not os.path.exists(os.path.join(root, "node1", "downloads", "gone.txt"))

        before = len(output["node1"])
        stop(procs["node2"])
        wait_until(
            lambda: any("peer removed: node2" in line for line in output["node1"]),
            10,
            "node2 was not removed\n" + "".join(output["node1"]),
        )
        send(procs["node1"], "files")
        wait_until(
            lambda: any(line.startswith("local shared:") for line in output["node1"][before:]),
            5,
            "files command after disconnect produced no table\n" + "".join(output["node1"][before:]),
        )
        # log lines start with a year; the files command does not
        shown = [line for line in output["node1"][before:] if not line[:1].isdigit()]
        assert any(line.startswith("local shared:") for line in shown), shown
        assert not any(line.startswith("node2 ") for line in shown), shown
        assert any(line.startswith("node3 ") for line in shown), shown
        assert not any("gone.txt" in line for line in shown), shown

        log_text = open(os.path.join(root, "node1", "log.txt"), encoding="utf-8").read()
        assert "SENT data_request to " in log_text
        assert "RECEIVED file_data from " in log_text
        assert "RECEIVED file_list from " in log_text
        assert "peer added:" in log_text
        assert "peer removed:" in log_text
        # file contents are not log records
        assert "from-node2" not in log_text
        print("ok: three nodes discovered each other, transferred a file, and dropped a leaver")
        print(textwrap.indent("".join(output["node1"][-30:]), "node1| "))
    finally:
        for proc in procs.values():
            stop(proc)
        for thread in threads:
            thread.join(timeout=2)


def threading_pump(proc, lines):
    import threading
    thread = threading.Thread(target=pump, args=(proc, lines), daemon=True)
    thread.start()
    return thread


if __name__ == "__main__":
    main()
