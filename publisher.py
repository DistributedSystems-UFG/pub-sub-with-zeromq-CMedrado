import zmq

# Constantes
HOST = "172.31.48.169"
TOPIC_PORT = 5556
RPC_PORT = 5557

# Inicializa o contexto ZeroMQ
context = zmq.Context()

# Cria o socket de pub para tópicos
topic_socket = context.socket(zmq.PUB)
topic_socket.bind("tcp://{}:{}".format(HOST, TOPIC_PORT))

# Cria o socket de rep para RPC
rpc_socket = context.socket(zmq.REP)
rpc_socket.bind("tcp://{}:{}".format(HOST, RPC_PORT))

# Inicializa o dicionário de tópicos
topics = {}

# Loop principal
while True:
    # Verifica se há mensagens RPC
    try:
        message = rpc_socket.recv_json(flags=zmq.NOBLOCK)
        if message["type"] == "individual":
            # Envia a mensagem individual para o destinatário
            dest = message["to"]
            msg = message["message"]
            topic_socket.send_multipart([dest.encode("utf-8"), msg.encode("utf-8")])
            rpc_socket.send_json({"status": "ok"})
        elif message["type"] == "topic":
            # Publica a mensagem no tópico
            topic = message["topic"]
            msg = message["message"]
            topic_socket.send_multipart([topic.encode("utf-8"), msg.encode("utf-8")])
            rpc_socket.send_json({"status": "ok"})
    except zmq.Again:
        pass

    # Recebe mensagens de tópicos
    try:
        topic, message = topic_socket.recv_multipart(flags=zmq.NOBLOCK)
        # Envia a mensagem para todos os usuários inscritos no tópico
        if topic in topics:
            for subscriber in topics[topic]:
                topic_socket.send_multipart([subscriber.encode("utf-8"), message])
    except zmq.Again:
        pass

    # Verifica se há novas conexões
    try:
        conn = rpc_socket.recv(flags=zmq.NOBLOCK)
        rpc_socket.send_json({"status": "ok"})
    except zmq.Again:
        pass

    # Verifica se há novas inscrições em tópicos
    try:
        topic, subscriber = topic_socket.recv_multipart(flags=zmq.NOBLOCK)
        # Adiciona o usuário à lista de inscritos no tópico
        if topic not in topics:
            topics[topic] = set()
        topics[topic].add(subscriber.decode("utf-8"))
    except zmq.Again:
        pass
