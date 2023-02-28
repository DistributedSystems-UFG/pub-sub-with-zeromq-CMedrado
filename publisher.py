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
        topic = msg_pack[1]
        src = msg_pack[2]
        msg = msg_pack[3]
        # Publica a mensagem no tópico
        topic_socket = self.context.socket(zmq.PUB)
        topic_socket.bind("tcp://%s:%s" % (constPS.TOPIC_SERVER_HOST, constPS.TOPIC_SERVER_PORT))
        topic_socket.send_string(
            "%s %s:%s %s" % (topic, constPS.CHAT_SERVER_HOST, constPS.CHAT_SERVER_PORT, pickle.dumps((src, msg))))
        topic_socket.close()


# Servidor de tópicos
class TopicServer:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.socket.bind("tcp://%s:%s" % (constPS.TOPIC_SERVER_HOST, constPS.TOPIC_SERVER_PORT))

    def run(self):
        while True:
            message = self.socket.recv_string()
            topic, message = message.split(" ", 1)
            print("TOPIC MESSAGE: %s - FROM: %s" % (message, topic))
            # Envia a mensagem para todos os assinantes do tópico
            sub_socket = self.context.socket(zmq.REQ)
            for addr in constPS.topic_subscribers.get(topic, []):
                try:
                    sub_socket.connect("tcp://%s:%s" % addr)
                    sub_socket.send(message.encode())
                    sub_socket.recv()
                except:
                    print("Error: Subscriber %s is down" % str(addr))
            sub_socket.close()


if name == "main":
    chat_server = ChatServer()
    topic_server = TopicServer()
    chat_thread = threading.Thread(target=chat_server.run)
    topic_thread = threading.Thread(target=topic_server.run)

    chat_thread.start()
    topic_thread.start()

    chat_thread.join()
    topic_thread.join()
