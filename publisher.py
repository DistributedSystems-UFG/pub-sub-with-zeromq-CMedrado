import zmq
import time
import threading
from constPS import *

context = zmq.Context()
pub_socket = context.socket(zmq.PUB)
pub_address = f"tcp://{HOST}:{PORT}"
pub_socket.bind(pub_address)

def send_individual_message(dest, msg):
    req_socket = context.socket(zmq.REQ)
    req_address = f"tcp://{HOST}:{INDIVIDUAL_REQ_PORT}"
    req_socket.connect(req_address)
    req_socket.send_pyobj((dest, msg))
    ack = req_socket.recv_string()
    if ack != "ACK":
        print(f"Error sending message to {dest}")
    req_socket.close()

def send_topic_message(topic, msg):
    pub_socket.send_string(f"{topic} {msg}")

class RecvHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.client_socket = sock

    def run(self):
        while True:
            (conn, addr) = self.client_socket.accept()
            msg_type = conn.recv(1024).decode("utf-8")
            if msg_type == "I":
                dest, msg = conn.recv(1024).decode("utf-8").split(" ")
                send_individual_message(dest, msg)
                conn.send(b"ACK")
            elif msg_type == "T":
                topic, msg = conn.recv(1024).decode("utf-8").split(" ")
                send_topic_message(topic, msg)
                conn.send(b"ACK")
            else:
                conn.send(b"NACK")
            conn.close()
        return

server_sock = context.socket(zmq.REP)
server_address = f"tcp://{HOST}:{SERVER_REQ_PORT}"
server_sock.bind(server_address)

while True:
    msg_pack = server_sock.recv_pyobj()
    dest = msg_pack[0]
    msg = msg_pack[1]
    send_individual_message(dest, msg)
    server_sock.send_string("ACK")
