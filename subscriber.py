import zmq
import threading
import constPS

# Subscriber
def subscribe():
    context = zmq.Context()
    s = context.socket(zmq.SUB)
    s.connect("tcp://" + constPS.HOST + ":" + str(constPS.PORT))
    s.setsockopt(zmq.SUBSCRIBE, b'#')  # subscribe to all topics

    while True:
        msg = s.recv()
        print("TOPIC MSG: " + msg.decode("utf-8"))

# Client
def send_msg():
    context = zmq.Context()
    c = context.socket(zmq.REQ)
    c.connect("tcp://" + constPS.HOST + ":" + str(constPS.PORT))

    while True:
        cmd = input("Enter command (1 for individual message, 2 for topic message): ")
        if cmd == "1":
            dest = input("Enter destination: ")
            msg = input("Enter message: ")
            msg_pack = (msg, dest, "client")
            marshaled_msg_pack = pickle.dumps(msg_pack)
            c.send(marshaled_msg_pack)
            marshaled_reply = c.recv()
            reply = pickle.loads(marshaled_reply)
            if reply != "ACK":
                print("Error: Server did not accept the message (dest does not exist?)")
        elif cmd == "2":
            topic = input("Enter topic: ")
            msg = input("Enter message: ")
            s.send(str.encode("#" + topic + " " + msg))
        else:
            print("Invalid command")

# Start threads for subscriber and client
t1 = threading.Thread(target=subscribe)
t1.start()
t2 = threading.Thread(target=send_msg)
t2.start()
