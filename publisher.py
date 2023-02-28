import zmq
import threading
import constPS
import pickle

context = zmq.Context()
s = context.socket(zmq.PUB)  # create a publisher socket
p = "tcp://" + constPS.HOST + ":" + str(constPS.PORT)  # how and where to communicate
s.bind(p)  # bind socket to the address


class RecvHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.client_socket = sock

    def run(self):
        while True:
            (conn, addr) = self.client_socket.accept()
            marshaled_msg_pack = conn.recv(1024)
            msg_pack = pickle.loads(marshaled_msg_pack)
            print("MESSAGE: " + msg_pack[0] + " - FROM: " + msg_pack[1])
            conn.send(pickle.dumps("ACK"))
            conn.close()
        return


def handle_client(conn, addr):
    # Recebe a mensagem do cliente
    marshaled_msg_pack = conn.recv(1024)
    msg_pack = pickle.loads(marshaled_msg_pack)
    msg = msg_pack[0]
    dest = msg_pack[1]
    src = msg_pack[2]
    # Imprime informações sobre a mensagem
    print("RELAYING MSG: " + msg + " - FROM: " + src + " - TO: " + dest)

    # Verifica se é uma mensagem para um tópico ou uma mensagem individual
    if dest.startswith("#"):  # se começa com "#" é mensagem para um tópico
        s.send(str.encode(dest + " " + msg))
    else:  # caso contrário é uma mensagem individual
        # Verifica se o destinatário está registrado no servidor
        try:
            dest_addr = constPS.registry[dest]
        except:
            conn.send(pickle.dumps("NACK"))
            conn.close()
            return

        # Cria uma conexão com o cliente destinatário
        client_sock = context.socket(zmq.REQ)
        dest_ip = dest_addr[0]
        dest_port = dest_addr[1]

        # Tenta se conectar ao cliente destinatário
        try:
            client_sock.connect("tcp://" + dest_ip + ":" + str(dest_port))
        except:
            print("Error: Destination client is down")
            client_sock.close()
            return

        # Envia a mensagem para o cliente destinatário e aguarda um ACK
        msg_pack = (msg, src)
        marshaled_msg_pack = pickle.dumps(msg_pack)
        client_sock.send(marshaled_msg_pack)
        marshaled_reply = client_sock.recv()
        reply = pickle.loads(marshaled_reply)
        if reply != "ACK":
            print("Error: Destination client did not receive message properly")

        # Fecha a conexão com o cliente destinatário
        client_sock.close()

    # Envia uma mensagem de ACK ao cliente que enviou a mensagem
    conn.send(pickle.dumps("ACK"))
    conn.close()


server_sock = context.socket(zmq.REP)
server_sock.bind("tcp://" + constPS.HOST + ":" + str(constPS.PORT))

# Put receiving thread to run
recv_handler = RecvHandler(server_sock)
recv_handler.start()

print("Chat Server is ready...")

while True:
    cmd = input("Enter command (1 for individual message, 2 for topic message): ")
    if cmd == "1":
        dest = input("Enter destination: ")
        msg = input("Enter message: ")
        msg_pack = (msg, dest, "server")

        # Cria uma conexão com o servidor
        client_sock = context.socket(zmq.REQ)
        server_ip = constPS.registry["server"][0]
        server_port = constPS.registry["server"][1]

        # Tenta se conectar ao servidor
        try:
            client_sock.connect("tcp://" + server_ip + ":" + str(server_port))
        except:
            print("Error: Server is down")
            client_sock.close()
            continue

        # Envia a mensagem para o servidor e aguarda um ACK
        marshaled_msg_pack = pickle.dumps(msg_pack)
        client_sock.send(marshaled_msg_pack)
        marshaled_reply = client_sock.recv()
        reply = pickle.loads(marshaled_reply)
        if reply != "ACK":
            print("Error: Server did not accept the message (dest does not exist?)")

        # Fecha a conexão com o servidor
        client_sock.close()

    elif cmd == "2":
        topic = input("Enter topic: ")
        msg = input("Enter message: ")
        s.send(str.encode("#" + topic + " " + msg))
    else:
        print("Invalid command. Please try again.")
