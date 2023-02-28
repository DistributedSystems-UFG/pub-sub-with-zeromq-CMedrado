import zmq
import threading
import constPS

class TopicRegistry:
    def __init__(self):
        self.topics = {}

    def add_subscriber(self, topic, address):
        if topic not in self.topics:
            self.topics[topic] = []
        self.topics[topic].append(address)

    def remove_subscriber(self, topic, address):
        if topic in self.topics and address in self.topics[topic]:
            self.topics[topic].remove(address)
            if not self.topics[topic]:
                del self.topics[topic]

    def get_subscribers(self, topic):
        return self.topics.get(topic, [])

# Servidor de chat
class ChatServer:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://%s:%s" % (constPS.CHAT_SERVER_HOST, constPS.CHAT_SERVER_PORT))
        self.topic_registry = TopicRegistry()

    def run(self):
        while True:
            marshaled_msg_pack = self.socket.recv()
            msg_pack = pickle.loads(marshaled_msg_pack)
            dest_type = msg_pack[0]
            if dest_type == "user":
                self.handle_user_msg(msg_pack)
            elif dest_type == "topic":
                self.handle_topic_msg(msg_pack)

    def handle_user_msg(self, msg_pack):
        topic = msg_pack[1]
        src = msg_pack[2]
        msg = msg_pack[3]
        # Publica a mensagem no tópico
        topic_socket = self.context.socket(zmq.PUB)
        topic_socket.bind("tcp://%s:%s" % (constPS.TOPIC_SERVER_HOST, constPS.TOPIC_SERVER_PORT))
        topic_socket.send_string("%s %s:%s %s" % (topic, constPS.CHAT_SERVER_HOST, constPS.CHAT_SERVER_PORT, pickle.dumps((src, msg))))
        # Adiciona os endereços dos clientes inscritos ao tópico
        subscribers = self.topic_registry.get_subscribers(topic)
        for address in subscribers:
            topic_socket.connect("tcp://%s:%s" % address)
            # Publica a mensagem para os clientes inscritos e fecha o socket
            topic_socket.send_string("%s %s:%s %s" % (topic, constPS.CHAT_SERVER_HOST, constPS.CHAT_SERVER_PORT, pickle.dumps((src, msg))))
        topic_socket.close()

    def handle_topic_msg(self, msg_pack):
        cmd = msg_pack[1]
        if cmd == "add_topic":
            topic = msg_pack[2]
            if topic not in constPS.topics:
                constPS.topics.append(topic)
                self.socket.send(pickle.dumps(("ACK", "Topic added successfully")))
            else:
                self.socket.send(pickle.dumps(("ACK", "Topic already exists")))
        elif cmd == "remove_topic":
            topic = msg_pack[2]
            if topic in constPS.topics:
                constPS.topics.remove(topic)
                self.socket.send(pickle.dumps(("ACK", "Topic removed successfully")))
            else:
                self.socket.send(pickle.dumps(("ACK", "Topic does not exist")))
        elif cmd == "list_topics":
            self.socket.send(pickle.dumps(("ACK", constPS.topics)))
        else:
            self.socket.send(pickle.dumps(("ACK", "Invalid command")))

# Servidor de tópicos
class TopicServer:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.socket.bind("tcp://%s:%s" % (constPS.TOPIC_SERVER_HOST, constPS.TOPIC_SERVER_PORT))
        self.topics = {}

    def run(self):
        while True:
            message = self.socket.recv_string()
            topic, message = message.split(" ", 1)
            print("TOPIC MESSAGE: %s - FROM: %s" % (message, topic))
            
    def add_topic(self, topic):
        if topic not in self.topics:
            self.topics[topic] = []
            print("Topic %s has been added" % topic)
        else:
            print("Topic %s already exists" % topic)

    def remove_topic(self, topic):
        if topic in self.topics:
            del self.topics[topic]
            print("Topic %s has been removed" % topic)
        else:
            print("Topic %s does not exist" % topic)

    def query_topic(self, topic):
        if topic in self.topics:
            print("Messages for topic %s:" % topic)
            for message in self.topics[topic]:
                print("- %s" % message)
        else:
            print("Topic %s does not exist" % topic)       

# Programa principal
if __name__ == "__main__":
    chat_server = ChatServer()
    topic_server = TopicServer()
    
    add_topic_thread = threading.Thread(target=lambda: topic_server.add_topic("test"))
    remove_topic_thread = threading.Thread(target=lambda: topic_server.remove_topic("test"))
    query_topic_thread = threading.Thread(target=lambda: topic_server.query_topic("test"))

    add_topic_thread.start()
    remove_topic_thread.start()
    query_topic_thread.start()

    add_topic_thread.join()
    remove_topic_thread.join()
    query_topic_thread.join()

    chat_thread = threading.Thread(target=chat_server.run)
    topic_thread = threading.Thread(target=topic_server.run)

    chat_thread.start()
    topic_thread.start()

    chat_thread.join()
    topic_thread.join()
