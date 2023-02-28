import zmq, time
import threading
import pickle
from constPS import *

# Server to receive individual messages
def handle_client(conn, addr):
    # Receives message from the client
    marshaled_msg_pack = conn.recv(1024)
    msg_pack = pickle.loads(marshaled_msg_pack)
    msg = msg_pack[0]
    dest = msg_pack[1]
    src = msg_pack[2]

    # Prints information about the message
    print("MESSAGE: " + msg + " - FROM: " + src + " - TO: " + dest)

    # Checks if the destination is registered in the server
    try:
        dest_addr = registry[dest]
    except:
        conn.send(pickle.dumps("NACK"))
        conn.close()
        return

    # Sends an ACK message to the client who sent the message
    conn.send(pickle.dumps("ACK"))
    conn.close()

    # Creates a connection with the destination client
    client_sock = zmq.Context().socket(zmq.REQ)
    dest_ip = dest_addr[0]
    dest_port = dest_addr[1]

    # Tries to connect to the destination client
    try:
        client_sock.connect("tcp://" + dest_ip + ":" + str(dest_port))
    except:
        print("Error: Destination client is down")
        client_sock.close()
        return

    # Sends the message to the destination client and waits for an ACK
    msg_pack = (msg, src)
    marshaled_msg_pack = pickle.dumps(msg_pack)
    client_sock.send(marshaled_msg_pack)
    marshaled_reply = client_sock.recv()
    reply = pickle.loads(marshaled_reply)
    if reply != "ACK":
        print("Error: Destination client did not receive message properly")

    # Closes the connection with the destination client
    client_sock.close()

# Publisher to send messages to topics
def publish():
    context = zmq.Context()
    s = context.socket(zmq.PUB)
    p = "tcp://" + PUB_HOST + ":" + PUB_PORT
    s.bind(p)
    while True:
        topic = input("ENTER TOPIC (PRESS ENTER FOR NO TOPIC): ")
        if topic == "":
            topic = "ALL"
        msg = input("ENTER MESSAGE: ")
        msg_pack = (msg, topic)
        marshaled_msg_pack = pickle.dumps(msg_pack)
        s.send(marshaled_msg_pack)

# Put receiving thread to run
recv_handler = threading.Thread(target=recv_messages)
recv_handler.start()

# Put publishing thread to run
pub_handler = threading.Thread(target=publish)
pub_handler.start()
