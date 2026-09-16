import socket
import sys
import threading
import time
import uuid
import json


# logging purposes
config_file = sys.argv[1]
log_filename = config_file.replace("config", "log")
log_file = open(log_filename, "w")


connections = {}
leader_id = None
done = False
buffer = ''

class Message:
    def __init__(self, uuid, flag):
        self.uuid = uuid
        self.flag = flag

def log(line):
    log_file.write(line + "\n")
    log_file.flush()

def compare_uuid(client_uuid):
    if client_uuid > my_uuid:
        return "greater"
    elif client_uuid < my_uuid:
        return "less"
    else:
        return "same"

def state():
    if leader_id is None:
        return "0"
    else:
        return f"1, leader_id={leader_id}"
 

# function to parse the config file (passed as command line argument)
def parse(config_file):
    with open(config_file) as f:
        lines = f.readlines()

    listen_addr, listen_port = lines[0].strip().split(",") # as a server, which addr & port i am listening on
    connect_addr, connect_port = lines[1].strip().split(",") # as a client, which addr & port am i connecting/sending info to

    listen_port = int(listen_port)
    connect_port = int(connect_port)

    return listen_addr, listen_port, connect_addr, connect_port


# function to set myself up as a server, listening on listen_addr and listen_port. 
# once a connection with client L is accepted, stop listening for new connections. new socket for continued communication with client L is created.
def start_server(listen_addr, listen_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock: 
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((listen_addr, listen_port))
        server_sock.listen(1)
        conn, addr = server_sock.accept() # conn = the socket the server uses to recieve data from client. addr = ip, port on client side.
        connections["conn"] = conn # save conn in dict


# function to set myself up as client. i as a client send data to neighboring server's ip addr and port
def start_client(connect_addr, connect_port):
    client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_sock.connect((connect_addr, connect_port))
    return client_sock

# function to send message to server as a client
def send_message(sock, msg):
    msg_dict = {"uuid": str(msg.uuid), "flag": msg.flag} # convert message object to dict
    json_str = json.dumps(msg_dict) # convert dict to json. json_str will be smth like: {"uuid": "1234", "flag": 0}
    sock.sendall(json_str.encode()) # convert json string to bytes and send through socket
    log(f"Sent: uuid={msg.uuid}, flag={msg.flag}")

# function to recieve message as a server from client
def receive_message(conn, buffer):
    while "}" not in buffer:
        data = conn.recv(1024)
        if not data:
            return None, None, buffer  
        buffer += data.decode()
 
    msg_end = buffer.index("}") + 1
    msg = buffer[:msg_end]
    buffer = buffer[msg_end:]

    msg_dict = json.loads(msg) # convert json string back to dict
    client_uuid = uuid.UUID(msg_dict["uuid"]) # pull out client's uuid
    client_flag = msg_dict["flag"] # pull out clients flag
 
    return client_uuid, client_flag, buffer

my_uuid = uuid.uuid4()
log(f"UUID: {my_uuid}")

listen_addr, listen_port, connect_addr, connect_port = parse(config_file)

# start seperate thread for start_server
server_thread = threading.Thread(target=start_server, args=(listen_addr, listen_port))
server_thread.start()

# main thread
# as start_server thread works in the background, do work
input("press Enter when everyone is ready.")
client_sock = start_client(connect_addr, connect_port)
    
# sending inital messsage
init_msg = Message(my_uuid, 0)
send_message(client_sock, init_msg)

server_thread.join() # wait here until start_server() finished (aka conn is ready to use)
conn = connections["conn"]

# as server, read what is coming from client
while not done:
    client_uuid, client_flag, buffer = receive_message(conn, buffer)
 
    if client_uuid is None:
        break
     
    log(f"Received: uuid={client_uuid}, flag={client_flag}, {compare_uuid(client_uuid)}, {state()}")

    # client flag = 1, leader is decided
    if client_flag == 1:
        leader_id = client_uuid
        log(f"Leader is decided to {leader_id}.")
 
        # if leader is not own uuid, send client uuid forward
        if client_uuid != my_uuid:
            send_message(client_sock, Message(client_uuid, 1))
        # if leader is own uuid, stop sending
        done = True
 
    # send client uuid forward
    elif client_uuid > my_uuid:
        send_message(client_sock, Message(client_uuid, 0))
 
    # ignore
    elif client_uuid < my_uuid:
        log(f"Ignored: uuid={client_uuid}, flag={client_flag}, {compare_uuid(client_uuid)}, {state()}")
 
    # i figured out i am leader because client_uuid == my_uuid
    else:  
        leader_id = my_uuid
        log(f"Leader is decided to {leader_id}.")
        send_message(client_sock, Message(my_uuid, 1))

log(f"leader is {leader_id}")
print(f"leader is {leader_id}") # for stdout
log_file.close()