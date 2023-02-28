import zmq
import time
import threading
from constPS import *

context = zmq.Context()

# Initialize publisher socket
publisher_socket = context.socket(zmq.PUB)
publisher_address = f"tcp://{HOST}:{PUBLISHER_PORT}"
publisher_socket.bind(publisher_address)

# Initialize subscriber socket
subscriber_socket = context.socket(zmq.SUB)
subscriber_address = f"tcp://{HOST}:{SUBSCRIBER_PORT}"
subscriber_socket.bind(subscriber_address)

# Initialize RPC socket
rpc_socket = context.socket(zmq.REP)
rpc_address = f"tcp://{HOST}:{RPC_PORT}"
rpc_socket.bind(rpc_address)


# Handler for receiving messages from individual clients
class IndividualMessageHandler(threading.Thread):
    def __init__(self, socket):
        threading.Thread.__init__(self)
        self.socket = socket

    def run(self):
        while True:
            message = self.socket.recv_json()
            topic = message['to']
            publisher_socket.send_string(f"{topic} {message['message']}")
            self.socket.send_json({'status': 'OK'})
        return


# Handler for receiving messages from topics
class TopicMessageHandler(threading.Thread):
    def __init__(self, socket):
        threading.Thread.__init__(self)
        self.socket = socket

    def run(self):
        while True:
            message = self.socket.recv_json()
            topic = message['to']
            publisher_socket.send_string(f"{topic} {message['message']}")
            self.socket.send_json({'status': 'OK'})
        return


# Handler for receiving RPC calls
class RPCHandler(threading.Thread):
    def __init__(self, socket):
        threading.Thread.__init__(self)
        self.socket = socket

    def run(self):
        while True:
            message = self.socket.recv_json()
            topic = message['to']
            publisher_socket.send_string(f"{topic} {message['message']}")
            self.socket.send_json({'status': 'OK'})
        return


# Start individual message handler
individual_handler = IndividualMessageHandler(subscriber_socket)
individual_handler.start()

# Start topic message handler
topic_handler = TopicMessageHandler(subscriber_socket)
topic_handler.start()

# Start RPC handler
rpc_handler = RPCHandler(rpc_socket)
rpc_handler.start()

# Main loop
while True:
    # Prompt user for command
    command = input("Enter command (i for individual, t for topic): ")

    if command == 'i':
        # Send message to an individual client
        to = input("Enter recipient's ID: ")
        message = input("Enter message: ")
        rpc_socket.send_json({'to': to, 'message': message})
        reply = rpc_socket.recv_json()
        if reply['status'] == 'OK':
            print("Message sent successfully")
        else:
            print("Error sending message")

    elif command == 't':
        # Send message to a topic
        topic = input("Enter topic: ")
        message = input("Enter message: ")
        publisher_socket.send_string(f"{topic} {message}")
        print("Message sent successfully")
    else:
        print("Invalid command, please try again.")
