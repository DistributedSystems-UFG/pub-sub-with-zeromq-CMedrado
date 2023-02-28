import zmq
import time
from constPS import *

context = zmq.Context()

# Socket publisher
s = context.socket(zmq.PUB)
p = "tcp://{}:{}".format(HOST, PORT)
s.bind(p)

# Socket RPC
rpc_socket = context.socket(zmq.REP)
rpc_socket.bind("tcp://*:{}".format(RPC_PORT))

# Dicionário para armazenar as conexões dos clientes
clients = {}

while True:
    # Recebe o comando do cliente
    command = rpc_socket.recv_json()

    # Verifica o tipo de mensagem
    if command['type'] == 'individual':
        # Envia mensagem individual
        dest = command['dest']
        message = command['message']

        if dest not in clients:
            rpc_socket.send_json({'status': 'error', 'message': 'User not found'})
        else:
            dest_socket = clients[dest]
            dest_socket.send_string(message)
            rpc_socket.send_json({'status': 'ok'})

    elif command['type'] == 'topic':
        # Envia mensagem para o tópico
        topic = command['topic']
        message = command['message']

        s.send_string("{} {}".format(topic, message))
        rpc_socket.send_json({'status': 'ok'})

    elif command['type'] == 'register':
        # Registra um novo cliente
        username = command['username']
        client_socket = context.socket(zmq.PAIR)
        client_socket.bind("inproc://{}".format(username))
        clients[username] = client_socket
        rpc_socket.send_json({'status': 'ok'})

    elif command['type'] == 'exit':
        # Remove um cliente registrado
        username = command['username']
        if username in clients:
            client_socket = clients[username]
            client_socket.close()
            del clients[username]
            rpc_socket.send_json({'status': 'ok'})
        else:
            rpc_socket.send_json({'status': 'error', 'message': 'User not found'})

    else:
        rpc_socket.send_json({'status': 'error', 'message': 'Invalid command'})

    # Publica a hora atual a cada 5 segundos
    current_time = time.asctime()
    s.send_string("TIME {}".format(current_time))
    time.sleep(5)
