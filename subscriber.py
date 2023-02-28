import zmq
import threading
import sys
from constPS import *

# subscriber client
class SubscribeClient(threading.Thread):
    def __init__(self, topic):
        threading.Thread.__init__(self)
        self.topic = topic

    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.connect("tcp://{}:{}".format(HOST, PORT))
        socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)
        while True:
            message = socket.recv()
            print("Message received on topic '{}': {}".format(self.topic, message))


# RPC client
def send_message():
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://{}:{}".format(HOST, PORT))
    print("To send a message, please enter the destination and the message separated by a comma.")
    print("For example, to send a message to user 'bob', type: bob,hello!")
    while True:
        try:
            user_input = input("Enter destination and message: ")
            dest, msg = user_input.split(",")
            socket.send_string(dest, zmq.SNDMORE)
            socket.send_string(msg)
            reply = socket.recv_string()
            if reply == "ACK":
                print("Message sent successfully to {}.".format(dest))
            else:
                print("Failed to send message to {}.".format(dest))
        except KeyboardInterrupt:
            print("Exiting...")
            break
        except Exception as e:
            print("Error: {}".format(e))
            continue


if __name__ == '__main__':
    try:
        while True:
            command = input("Enter '1' to subscribe to a topic, '2' to send a message, or 'q' to quit: ")
            if command == '1':
                topic = input("Enter topic to subscribe to: ")
                sub_client = SubscribeClient(topic)
                sub_client.start()
            elif command == '2':
                send_message()
            elif command == 'q':
                print("Exiting...")
                break
            else:
                print("Invalid command.")
    except KeyboardInterrupt:
        print("Exiting...")
