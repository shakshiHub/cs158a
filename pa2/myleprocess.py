import socket
import sys
import threading
import time
import uuid
import json


my_uuid = uuid.uuid4()

class Message:
    def __init__(self, uuid, flag):
        self.uuid = uuid
        self.flag = flag

def parse(config_file):
    with open(config_file) as f:
        lines = f.readlines()

    server_addr, server_port = lines[0].strip().split(",")
    client_addr, client_port = lines[1].strip().split(",")

    server_port = int(server_port)
    client_port = int(client_port)

    return server_addr, server_port, client_addr, client_port


config_file = sys.argv[1]
server_addr, server_port, client_addr, client_port = parse(config_file)

# setting up as server to listen 
def handle_client():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock: # setting up socket as a server 
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((server_addr, server_port))
        server_sock.listen(1)
        conn, addr = server_sock.accept() # conn = new socket where server and that specific client will communicate on. addr = ip, port on client side.
        print("Server accepted connection")

        while True:
            data = conn.recv(1024)
            if not data:
                print("Connection closed by client")
                break

            received_str = data.decode()
            received_dict = json.loads(received_str)
            received_uuid = uuid.UUID(received_dict["uuid"])
            received_flag = received_dict["flag"]

            print(f"Received uuid={received_uuid}, flag={received_flag}")


server_thread = threading.Thread(target=handle_client)
server_thread.start()

# setting up as client to talk
time.sleep(10)
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_sock:
    client_sock.connect((client_addr, client_port))
    print("Client connected")
    
    init_msg = Message(my_uuid, 0)
    msg_dict = {"uuid": str(init_msg.uuid), "flag": init_msg.flag}
    json_str = json.dumps(msg_dict)
    client_sock.sendall(json_str.encode())
    print("Sent initial message")


