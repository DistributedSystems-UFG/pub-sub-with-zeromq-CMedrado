import zmq
import threading
import time
import sys
from constPS import *

# Subscribing to topic(s)
def subscribe():
    context = zmq.Context()
    s = context.socket(zmq.SUB)
    s.connect("tcp://" + HOST + ":" + PORT)
    topics = input("Enter topic(s) to subscribe (separated by space): ")
    topics_list = topics.split()
    for t in topics_list:
        s.setsockopt_string(zmq.SUBSCRIBE, t)
    while True:
        msg = s.recv()
        print("TOPIC: " + msg.decode())

# Sending individual messages
def send_message():
    context = zmq.Context()
    s = context.socket(zmq.REQ)
    s.connect("tcp://" + HOST + ":" + PORT)
    while True:
        dest = input("ENTER DESTINATION: ")
        msg = input("ENTER MESSAGE: ")
        msg_pack = (msg, dest)
        marshaled_msg_pack = zmq.utils.jsonapi.dumps(msg_pack)
        s.send(marshaled_msg_pack)
        marshaled_reply = s.recv()
        reply = zmq.utils.jsonapi.loads(marshaled_reply)
        if reply != "ACK":
            print("Error: Server did not accept the message (dest does not exist?)")
        else:
            pass

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Error: Please specify 'sub' or 'send'")
        sys.exit(1)

    if sys.argv[1] == "sub":
        subscribe()
    elif sys.argv[1] == "send":
        send_message()
    else:
        print("Error: Invalid argument")
        sys.exit(1)
