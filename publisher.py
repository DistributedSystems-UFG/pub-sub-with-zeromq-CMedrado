import zmq
import time
import threading
import pickle
from constPS import *

class RecvHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.client_socket = sock

    def run(self):
        while True:
            (conn, addr) = self.client_socket.accept()
            marshaled_msg_pack = conn.recv(1024)
            msg_pack = pickle.loads(marshaled_msg_pack)
            msg = msg_pack[0]
            dest = msg_pack[1]
            src = msg_pack[2]
            # Imprime informações sobre a mensagem
            print("MESSAGE: " + msg + " - FROM: " + src + " - TO: " + dest)

            # Verifica se o destinatário está registrado no servidor
            try:
                dest_addr = registry[dest]
            except:
                conn.send(pickle.dumps("NACK"))
                conn.close()
                continue

            # Envia uma mensagem de ACK ao cliente que enviou a mensagem  
            conn.send(pickle.dumps("ACK"))
            conn.close()

            # Cria uma conexão com o cliente destinatário
            client_sock = socket(AF_INET, SOCK_STREAM)
            dest_ip = dest_addr[0]
            dest_port = dest_addr[1]

            # Tenta se conectar ao cliente destinatário
            try:
                client_sock.connect((dest_ip, dest_port))
            except:
                print("Error: Destination client is down")
                client_sock.close()
                continue

            # Envia a mensagem para o cliente destinatário e aguarda um ACK
            msg_pack = (msg, src)
            marshaled_msg_pack = pickle.dumps(msg_pack)
            client_sock.send(marshaled_msg_pack)
            marshaled_reply = client_sock.recv(1024)
            reply = pickle.loads(marshaled_reply)
            if reply != "ACK":
                print("Error: Destination client did not receive message properly")

            # Fecha a conexão com o cliente destinatário
            client_sock.close()
        return

def publish_message():
    context = zmq.Context()
    s = context.socket(zmq.PUB)        # create a publisher socket
    p = "tcp://"+ HOST +":"+ PORT      # how and where to communicate
    s.bind(p)                          # bind socket to the address

    while True:
        message_type = input("Enter message type (user/topic): ")
        if message_type == "user":
            dest = input("Enter destination username: ")
        elif message_type == "topic":
            dest = input("Enter topic: ")
        else:
            print("Invalid message type. Try again.")
            continue
        
        msg = input("Enter message: ")
        msg_pack = (msg, dest)
        marshaled_msg_pack = pickle.dumps(msg_pack)
        s.send(marshaled_msg_pack)
        print("Message sent to " + dest)

if __name__ == "__main__":
    # Cria o socket do servidor
    server_sock = socket(AF_INET, SOCK_STREAM)
    server_sock.bind((CHAT_SERVER_HOST, CHAT_SERVER_PORT))
    server_sock.listen(5)
    print("Chat Server is ready...")

    # Cria a thread para receber mensagens
    recv_handler = RecvHandler(server_sock)
    recv_handler.start()

    # Cria a thread para publicar mensagens
    pub_thread = threading.Thread(target=publish_message)
    pub_thread.start()
