import zmq
import threading
import constPS
import pickle

class Server(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://%s:%s" % (constPS.CHAT_SERVER_HOST, constPS.CHAT_SERVER_PORT))
        print("Chat server is running...")

    def run(self):
        while True:
            # Recebe mensagem do cliente
            marshaled_msg_pack = self.socket.recv()
            msg_pack = pickle.loads(marshaled_msg_pack)
            msg_type = msg_pack[0]
            msg = msg_pack[1]
            dest = msg_pack[2]
            src = msg_pack[3]

            # Mensagem individual
            if msg_type == "I":
                # Encontra o IP e porta do destinatário
                try:
                    dest_addr = constPS.registry[dest]
                except:
                    self.socket.send(pickle.dumps("NACK"))
                    continue

                # Cria conexão com o destinatário e envia a mensagem
                try:
                    client_sock = self.context.socket(zmq.REQ)
                    client_sock.connect("tcp://%s:%s" % (dest_addr[0], dest_addr[1]))
                    msg_pack = ("I", msg, src)
                    marshaled_msg_pack = pickle.dumps(msg_pack)
                    client_sock.send(marshaled_msg_pack)
                    marshaled_reply = client_sock.recv()
                    reply = pickle.loads(marshaled_reply)
                    client_sock.close()
                except:
                    reply = "NACK"

            # Mensagem para tópico
            elif msg_type == "T":
                # Envia a mensagem para todos os assinantes do tópico
                try:
                    publisher_sock = self.context.socket(zmq.PUB)
                    publisher_sock.bind("tcp://%s:%s" % (constPS.PUBLISHER_HOST, constPS.PUBLISHER_PORT))
                    msg_pack = ("T", msg)
                    marshaled_msg_pack = pickle.dumps(msg_pack)
                    publisher_sock.send_multipart([bytes(dest, "utf-8"), marshaled_msg_pack])
                    publisher_sock.close()
                    reply = "ACK"
                except:
                    reply = "NACK"

            # Responde ao cliente
            self.socket.send(pickle.dumps(reply))


class Publisher(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind("tcp://%s:%s" % (constPS.PUBLISHER_HOST, constPS.PUBLISHER_PORT))

    def run(self):
        while True:
            dest = input("Enter topic to publish message to: ")
            msg = input("Enter message: ")
            msg_pack = ("T", msg)
            marshaled_msg_pack = pickle.dumps(msg_pack)
            self.socket.send_multipart([bytes(dest, "utf-8"), marshaled_msg_pack])


if __name__ == "__main__":
    # Inicializa servidor e publicador
    server = Server()
    server.start()
    publisher = Publisher()
    publisher.start()
