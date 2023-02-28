import zmq
import pickle

class RegistryServer:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.registry = {}

    def run(self):
        self.socket.bind("tcp://*:5555")
        while True:
            # Espera uma solicitação de registro do cliente
            marshaled_request = self.socket.recv()
            request = pickle.loads(marshaled_request)
            if request[0] == "register":
                # Registra o cliente na tabela hash
                self.registry[request[1]] = (request[2], request[3])
                reply = "ACK"
            elif request[0] == "get_registry":
                # Retorna a tabela hash para o cliente
                reply = self.registry
            else:
                reply = "Invalid request"
            # Envia a resposta de volta para o cliente
            marshaled_reply = pickle.dumps(reply)
            self.socket.send(marshaled_reply)
