import zmq
import threading
import constPS

# Servidor de chat
class ChatServer:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://%s:%s" % (constPS.CHAT_SERVER_HOST, constPS.CHAT_SERVER_PORT))

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
        dest = msg_pack[1]
        src = msg_pack[2]
        msg = msg_pack[3]
        # Verifica se o destinatário está registrado no servidor
        try:
            dest_addr = constPS.registry[dest]
        except:
            self.socket.send(pickle.dumps(("ACK", "Destination user not found")))
            return
        # Cria uma conexão com o cliente destinatário
        client_sock = self.context.socket(zmq.REQ)
        dest_ip = dest_addr[0]
        dest_port = dest_addr[1]
        # Tenta se conectar ao cliente destinatário
        try:
            client_sock.connect("tcp://%s:%s" % (dest_ip, dest_port))
        except:
            self.socket.send(pickle.dumps(("ACK", "Destination user is down")))
            client_sock.close()
            return
        # Envia a mensagem para o cliente destinatário e aguarda um ACK
        msg_pack = (src, msg)
        marshaled_msg_pack = pickle.dumps(msg_pack)
        client_sock.send(marshaled_msg_pack)
        marshaled_reply = client_sock.recv()
        reply = pickle.loads(marshaled_reply)
        if reply != "ACK":
            self.socket.send(pickle.dumps(("ACK", "Destination user did not receive message properly")))
        else:
            self.socket.send(pickle.dumps(("ACK", "Message sent successfully")))

        client_sock.close()

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
