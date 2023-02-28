import zmq
import threading
import pickle
import constPS

class Subscriber:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)

    def subscribe(self, topic):
        self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)
        self.socket.connect("tcp://%s:%s" % (constPS.TOPIC_SERVER_HOST, constPS.TOPIC_SERVER_PORT))

    def run(self):
        while True:
            message = self.socket.recv_string()
            topic, message = message.split(" ", 1)
            print("TOPIC MESSAGE: %s - %s" % (topic, message))

if __name__ == "__main__":
    subscriber = Subscriber()

    while True:
        dest_type = input("Enter destination type (user or topic): ")
        if dest_type == "user":
            dest = input("Enter username: ")
            msg = input("Enter message: ")
            # Envia mensagem individual
            client_sock = subscriber.context.socket(zmq.REQ)
            try:
                dest_addr = constPS.registry[dest]
            except:
                print("Destination user not found")
                continue
            dest_ip = dest_addr[0]
            dest_port = dest_addr[1]
            try:
                client_sock.connect("tcp://%s:%s" % (dest_ip, dest_port))
            except:
                print("Destination user is down")
                client_sock.close()
                continue
            msg_pack = (dest_type, dest, constPS.CLIENT_NAME, msg)
            marshaled_msg_pack = pickle.dumps(msg_pack)
            client_sock.send(marshaled_msg_pack)
            marshaled_reply = client_sock.recv()
            reply = pickle.loads(marshaled_reply)
            if reply != "ACK":
                print("Destination user did not receive message properly")
            else:
                print("Message sent successfully")
            client_sock.close()
        elif dest_type == "topic":
            topic = input("Enter topic: ")
            msg = input("Enter message: ")
            # Publica mensagem em tópico
            topic_socket = subscriber.context.socket(zmq.PUB)
            topic_socket.connect("tcp://%s:%s" % (constPS.TOPIC_SERVER_HOST, constPS.TOPIC_SERVER_PORT))
            msg_pack = (dest_type, topic, constPS.CLIENT_NAME, msg)
            marshaled_msg_pack = pickle.dumps(msg_pack)
            topic_socket.send_string("%s %s:%s %s" % (topic, constPS.CHAT_SERVER_HOST, constPS.CHAT_SERVER_PORT, marshaled_msg_pack))
            topic_socket.close()
        else:
            print("Invalid destination type")
            continue

    subscriber_thread = threading.Thread(target=subscriber.run)
    subscriber_thread.start()
