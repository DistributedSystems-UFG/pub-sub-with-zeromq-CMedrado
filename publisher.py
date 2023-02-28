import zmq
import threading
import time
from constPS import *

def receive():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://{}:{}".format(HOST, PORT))

    while True:
        # Espera por uma mensagem
        message = socket.recv()

        # Analisa a mensagem recebida
        parts = message.decode().split(":")
        if len(parts) != 3:
            socket.send("ERRO: formato de mensagem inválido.".encode())
            continue

        # Envia a mensagem para o destinatário ou para o tópico correspondente
        if parts[0] == "INDIVIDUAL":
            # Envio de mensagem individual
            topic = parts[1]
            msg = parts[2]
            socket.send(b"OK")

            # Publica a mensagem no tópico correspondente
            publisher.send_string("{} {}".format(topic, msg))

        elif parts[0] == "TOPICO":
            # Envio de mensagem para tópico
            topic = parts[1]
            msg = parts[2]
            socket.send(b"OK")

            # Publica a mensagem no tópico correspondente
            publisher.send_string("{} {}".format(topic, msg))

        else:
            # Comando inválido
            socket.send("ERRO: formato de mensagem inválido.".encode())

def publish():
    context = zmq.Context()
    global publisher
    publisher = context.socket(zmq.PUB)
    publisher.bind("tcp://{}:{}".format(HOST, PORT))

    while True:
        # Publica a hora atual em um tópico
        current_time = time.strftime("%H:%M:%S", time.gmtime())
        message = "HORARIO {}".format(current_time)
        publisher.send_string("horario {}".format(message))
        time.sleep(1)

if __name__ == "__main__":
    # Inicia os threads de recepção e publicação
    threading.Thread(target=receive).start()
    threading.Thread(target=publish).start()

    # Espera indefinidamente
    while True:
        pass
