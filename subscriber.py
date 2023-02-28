import zmq
import time
import constPS


class Client:
    def __init__(self, name):
        self.name = name
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.SUBSCRIBE, b"")
        self.socket.connect("tcp://%s:%s" % (constPS.PUBLISHER_HOST, constPS.PUBLISHER_PORT))

    def subscribe_to_topic(self, topic):
        self.socket.setsockopt(zmq.SUBSCRIBE, topic.encode())

    def receive_messages(self):
        while True:
            msg = self.socket.recv()
            print(msg.decode())

    def send_message_to_user(self, dest, message):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://%s:%s" % (constPS.CHAT_SERVER_HOST, constPS.CHAT_SERVER_PORT))
        msg_pack = (message, dest, self.name)
        marshaled_msg_pack = pickle.dumps(msg_pack)
        socket.send(marshaled_msg_pack)
        marshaled_reply = socket.recv()
        reply = pickle.loads(marshaled_reply)
        if reply != "ACK":
            print("Error: Server did not accept the message (dest does not exist?)")
        else:
            print("Message sent successfully!")
        socket.close()


if __name__ == "__main__":
    client_name = input("Enter your name: ")
    client = Client(client_name)

    while True:
        msg_type = input("Enter message type (1 - individual, 2 - topic): ")
        if msg_type == "1":
            dest = input("Enter destination user: ")
            message = input("Enter message: ")
            client.send_message_to_user(dest, message)
        elif msg_type == "2":
            topic = input("Enter topic to subscribe: ")
            client.subscribe_to_topic(topic)
            print("Subscribed to topic %s" % topic)
            client.receive_messages()
        else:
            print("Invalid message type. Try again.")
