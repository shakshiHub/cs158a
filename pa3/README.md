# CS 158A P2P Data Sharing Agent

Run this from `cs158a/pa3`. UDP is only for discovery. TCP carries every message after that.

## How to run the agent

```bash
python3 agent.py --port 5001 --node-id node1
```

`--port` is this node's TCP port. `--node-id` is the name other nodes see. `--udp-port` defaults to 54321. `--workdir` defaults to the current directory (`shared/`, `downloads/`, and `log.txt`).

Commands: `peers`, `files`, `request <peer_id> <filename>`, `help`, `quit`.

On one computer, each node needs its own TCP port and its own `--workdir`. All of them use UDP 54321.

```bash
python3 agent.py --port 5001 --node-id node1 --workdir demo_nodes/node1
python3 agent.py --port 5002 --node-id node2 --workdir demo_nodes/node2
```

On different computers, use the same UDP port 54321 and any TCP port. The machines must be on the same LAN so the broadcast reaches them.

## Message format

### How message boundaries are determined

A UDP datagram is one JSON object. It has no length prefix.

A TCP message is:

1. 4 bytes, unsigned, big-endian (`!I`): the length of the JSON header.
2. That many UTF-8 JSON bytes.
3. `payload_len` raw bytes after the header. This is 0 when the message has no file.

`recv()` may return less than requested; the reader continues until the full length arrives. `b""` means the peer closed the connection. A header over 64 KB, or a payload over 1 GB, is rejected.

Every TCP header includes `type`, `from` (sender node id), and `payload_len`.

### `peer_request` (UDP)

```json
{"type": "peer_request", "from": "node1"}
```

| Field | Meaning |
| --- | --- |
| `type` | `peer_request` |
| `from` | Node that is announcing itself |

### `peer_ack` (UDP)

```json
{"type": "peer_ack", "from": "node2", "to": "node1", "tcp_port": 5002}
```

| Field | Meaning |
| --- | --- |
| `type` | `peer_ack` |
| `from` | Node that is already running |
| `to` | Node this ack is for. Other nodes ignore it |
| `tcp_port` | TCP port the sender is listening on |

The new node connects to the packet's source IP on `tcp_port`.

### `file_list` (TCP)

Filenames only. `payload_len` is 0.

```json
{"type": "file_list", "from": "node1", "files": ["sample1.txt", "sample2.txt"], "payload_len": 0}
```

| Field | Meaning |
| --- | --- |
| `type` | `file_list` |
| `from` | Sender node id |
| `files` | Regular filenames in that node's `shared/` directory |
| `payload_len` | `0` |

### `data_request` (TCP)

```json
{"type": "data_request", "from": "node1", "filename": "unique5.txt", "payload_len": 0}
```

| Field | Meaning |
| --- | --- |
| `type` | `data_request` |
| `from` | Node asking for the file |
| `filename` | One base filename |
| `payload_len` | `0` |

### `file_data` (TCP)

Success: the file bytes follow the header. `payload_len` is the file size.

```json
{"type": "file_data", "from": "node5", "filename": "unique5.txt", "success": true, "payload_len": 23}
```

| Field | Meaning |
| --- | --- |
| `type` | `file_data` |
| `from` | Node that has the file |
| `filename` | Name that was requested |
| `success` | `true` |
| `payload_len` | Number of file bytes that follow |
| payload | The file itself |

Failure: no payload. The connection stays open.

```json
{"type": "file_data", "from": "node2", "filename": "gone.txt", "success": false, "error": "File not found", "payload_len": 0}
```

| Field | Meaning |
| --- | --- |
| `type` | `file_data` |
| `from` | Node that received the request |
| `filename` | Name that was requested |
| `success` | `false` |
| `error` | `File not found`, `Invalid filename`, or `File too large` |
| `payload_len` | `0` |

### How file content is encoded

The file is the raw bytes after the JSON header. They are not placed inside the JSON and they are not Base64. The header only names the file and gives `payload_len`.

## Division of work

- Shakshi Sharma: UDP discovery in `testing.py` (`peer_request`, `peer_ack`, and the TCP accept loop)
- Alisha Pol: TCP framing in `protocol.py`, the peer table in `peers.py`, per-peer connections in `connection.py`, logging in `logger.py`, and `test_connection.py`
- Vaishnavi Panchal: file-list exchange, file transfer, `agent.py`, `discovery.py`, `fileshare.py`, `test_files.py`, `test_integration.py`, and this README

## Execution example

Five nodes on 2026-10-20, UDP 54321, TCP 5001–5005, one unique file each. On node1 the commands were `peers`, `files`, `request node5 unique5.txt`, `files`, then `peers` and `files` again after node4 quit. The program does not echo the typed command.

```text
2026-10-20 01:50:11 EVENT node1 started; TCP 5001; UDP 54321; sharing /Users/vaisp/Downloads/cs158pa3/cs158a/pa3/demo_nodes/node1/shared
ready
commands:
  peers                         show connected peers
  files                         show the files each peer is advertising
  request <peer_id> <filename>  download a file that peer has advertised
  help                          show this message
  quit                          disconnect and exit
> 2026-10-20 01:50:12 EVENT incoming TCP connection from 10.251.200.16:56854
2026-10-20 01:50:12 EVENT peer added: node2 at 10.251.200.16:56854 (1 peer)
2026-10-20 01:50:12 EVENT file list from node2: unique2.txt
2026-10-20 01:50:12 EVENT incoming TCP connection from 10.251.200.16:56856
2026-10-20 01:50:12 EVENT peer added: node3 at 10.251.200.16:56856 (2 peers)
2026-10-20 01:50:12 EVENT file list from node3: unique3.txt
2026-10-20 01:50:12 EVENT incoming TCP connection from 10.251.200.16:56858
2026-10-20 01:50:12 EVENT peer added: node4 at 10.251.200.16:56858 (3 peers)
2026-10-20 01:50:12 EVENT file list from node4: unique4.txt
2026-10-20 01:50:12 EVENT incoming TCP connection from 10.251.200.16:56861
2026-10-20 01:50:12 EVENT peer added: node5 at 10.251.200.16:56861 (4 peers)
2026-10-20 01:50:12 EVENT file list from node5: unique5.txt
node2 10.251.200.16:56854 files=1
node3 10.251.200.16:56856 files=1
node4 10.251.200.16:56858 files=1
node5 10.251.200.16:56861 files=1
> local shared: unique1.txt
node2 10.251.200.16:56854: unique2.txt
node3 10.251.200.16:56856: unique3.txt
node4 10.251.200.16:56858: unique4.txt
node5 10.251.200.16:56861: unique5.txt
> 2026-10-20 01:50:12 EVENT requested unique5.txt from node5
requested unique5.txt from node5
> 2026-10-20 01:50:12 EVENT saved unique5.txt (23 bytes) to downloads/ and shared/
local shared: unique1.txt, unique5.txt
node2 10.251.200.16:56854: unique2.txt
node3 10.251.200.16:56856: unique3.txt
node4 10.251.200.16:56858: unique4.txt
node5 10.251.200.16:56861: unique5.txt
> 2026-10-20 01:50:13 EVENT peer removed: node4 at 10.251.200.16:56858, its 1 file(s) dropped (3 peers left)
node2 10.251.200.16:56854 files=1
node3 10.251.200.16:56856 files=1
node5 10.251.200.16:56861 files=1
> local shared: unique1.txt, unique5.txt
node2 10.251.200.16:56854: unique2.txt
node3 10.251.200.16:56856: unique3.txt
node5 10.251.200.16:56861: unique5.txt
```

`unique5.txt` was saved in node1's `downloads/` and `shared/`. After node4 quit, that peer and `unique4.txt` were gone. Node2 printed `file list from node1: unique1.txt, unique5.txt` once the transfer finished.
