import zmq
import threading
import constPS


def subscribe_topic():
    context = zmq.Context()
    sub = context.socket(zmq.SUB)
    sub.connect("tcp://{}:{}".format(constPS.HOST, constPS.PORT))

    # Subscribe to all topics
    sub.subscribe(b"")

    while True:
        msg = sub.recv().decode()
        print("[TOPIC] " + msg)


def send_message():
    context = zmq.Context()
    req = context.socket(zmq.REQ)
    req.connect("tcp://{}:{}".format(constPS.HOST, constPS.PORT))

    while True:
        dest = input("Enter destination ('topic' for topic): ")
        msg = input("Enter message: ")

        if dest == "topic":
            req.send_string("{}:{}".format(dest, msg))
        else:
            req.send_string("{}:{}".format(dest, msg))
            reply = req.recv().decode()
            print("[ACK] " + reply)


if __name__ == "__main__":
    t1 = threading.Thread(target=subscribe_topic)
    t1.start()

    send_message()
