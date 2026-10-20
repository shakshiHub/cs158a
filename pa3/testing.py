import socket
import json
import uuid
import sys
import threading

def create_udp_socket():
    '''create udp socket, bind to port 54321'''
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # create udp socket (for listening & broadcasting)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1) # allow sending broadcasts

    # let several nodes on one machine bind 54321 (for demo purposes)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    sock.bind(("", udp_port)) # bind the socket to port 54321
    
    return sock

def send_peer_request(sock):
    '''broadcast 'hey im new, who else is here' once'''
    peer_request = {"type": "peer_request", "from": node_id} # 'hey im new, who else is here?'
    sock.sendto(json.dumps(peer_request).encode(), ("255.255.255.255", udp_port)) # broadcast peer request to all nodes in local network


def udp_listener(sock):
    '''listen on port 54321 for the lifetime of the program '''
    while True:
        data, addr = sock.recvfrom(1024) # 1024 bytes max
        msg = json.loads(data.decode()) # raw bytes -> string -> dict
        
        if msg["from"] == node_id: # ignore own broadcast
            continue

        # if incoming message is a peer request from another node,  send an ACK
        if msg["type"] == "peer_request":
            print("peer request from", msg["from"], addr)
            send_peer_ack(sock, msg["from"])
        
        # if incoming message is an ACK for me, note who to connect to 
        elif msg["type"] == "peer_ack":
            if msg["to"] != node_id: # ACK meant for some other new node
                continue
            print("peer ack from", msg["from"], "at", addr[0], "tcp port", msg["tcp_port"]) # at is the ip the ACK came from
            connect_to_peer(addr[0], msg["tcp_port"]) # connect to the node that ACKed me
        


def send_peer_ack(sock, to_id):
    """reply to a new node: 'i'm here, connect to my tcp port.'"""
    peer_ack = {"type": "peer_ack", "from": node_id, "to": to_id, "tcp_port": tcp_port}
    sock.sendto(json.dumps(peer_ack).encode(), ("255.255.255.255", udp_port)) # broadcasted to everyone


def create_tcp_listener():
    '''create tcp socket that listens for incoming peer connections on tcp_port'''
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", tcp_port))
    sock.listen()
    return sock

def tcp_accept_loop(sock):
    '''accept incoming tcp connections from new nodes for the lifetime of the program'''
    while True:
        conn, addr = sock.accept() # wait for a new node to connect
        print("tcp connection from", addr)

def connect_to_peer(ip, port):
    '''as the new node, open a tcp connection to a peer that sent me an ACK'''
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # new tcp socket for this one peer
    sock.connect((ip, port)) # connect to the peer's tcp listener (their ip, and tcp port they told me about)
    print("connected to", ip, port)
    return sock


udp_port = 54321 # all nodes broadcast to and listen on port 54321
tcp_port = int(sys.argv[1]) # giving tcp port as cl arg as it can't be shared unlike udp ports
node_id = str(uuid.uuid4()) # random id for node

# setup
tcp_sock = create_tcp_listener() # create tcp socket, begin listening on it
udp_sock = create_udp_socket() # create udp socket

# start background thread
threading.Thread(target=tcp_accept_loop, args=(tcp_sock,), daemon=True).start() # accept new nodes wanting to connect on your tcp socket

# announce myself, listen on udp forever
send_peer_request(udp_sock)
udp_listener(udp_sock) # send ACKs to new joiners, handle ACKs sent back to me as a new joiner 