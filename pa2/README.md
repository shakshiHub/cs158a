# PA2: Leader Election

## Files

- `myleprocess.py`
- `config1.txt`, `config2.txt`, `config3.txt`
- `log1.txt`, `log2.txt`, `log3.txt` — resulting logs from local demo

## Config format

Each config file has two lines:

```
<my_ip>,<my_port>
<neighbor_ip>,<neighbor_port>
```

Line 1 is the address this process listens on as a server. Line 2 is the
address it connects to as a client.

## Local Demo

1. Open a terminal, `cd` into `cs158a/pa2/`, and run:
   ```
   python3 myleprocess.py config1.txt
   ```
2. Open a second terminal (split terminal) and run:
   ```
   python3 myleprocess.py config2.txt
   ```
3. Open a third terminal and run:
   ```
   python3 myleprocess.py config3.txt
   ```
4. Each terminal will pause with `press Enter when everyone is ready.`
   At this point your machine is set up as a server, listening for its
   neighbor to connect. Once you press Enter, your machine connects out
   as a client and messages can start getting passed around the ring.
   Once all three terminals are running and paused, go back to each one
   and press Enter.
5. The election runs automatically. Each process writes its own log file
   (`log1.txt`, `log2.txt`, `log3.txt`).

## In-class demo (Real Network Environment)

Same idea, but with a single `config.txt`:

- Line 1: my own IP and port, which I run my server on.
- Line 2: my neighbor's IP and port, exchanged in class.

Run:
```
python3 myleprocess.py config.txt
```
Then press Enter once everyone in the ring is ready.

## Execution example

Resulting `log1.txt`, `log2.txt`, and `log3.txt` from running local demo on config1.txt, config2.txt, config3.txt

<br>

<img width="516" height="149" alt="Screenshot 2026-09-15 at 11 58 06 PM" src="https://github.com/user-attachments/assets/3f987949-f89a-46d4-a5e6-5bbc9f7282eb" />

<br><br>

<img width="505" height="193" alt="Screenshot 2026-09-16 at 12 01 46 AM" src="https://github.com/user-attachments/assets/70a3e0e4-52e2-46b8-9c4c-b2293b8d1b77" />

<br><br>

<img width="497" height="166" alt="Screenshot 2026-09-16 at 12 01 59 AM" src="https://github.com/user-attachments/assets/4909d597-779a-48d1-8c8c-8e151b5ad7ea" />

<br>
<br>
All three processes agree on the same leader, indicating successful completion of program.
