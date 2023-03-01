import zmq
import threading
from constPS import *

context = zmq.Context()

# Subscriber
def subscriber():
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://" + SERVER_ADDR + ":" + TOPIC_PORT)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    while True:
        message = socket.recv_string()
        print(message)

# Client
def client():
    name = input("Digite seu nome: ")
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://" + SERVER_ADDR + ":" + SERVER_PORT)

    while True:
        # Menu de opções
        print("Escolha uma opção:")
        print("1. Enviar mensagem individual")
        print("2. Publicar mensagem em tópico")
        opcao = input("Opção escolhida: ")

        if opcao == "1":
            # Envio de mensagem individual
            dest = input("Destinatário: ")
            msg = input("Mensagem: ")
            socket.send_json({"op": "ind", "from": name, "to": dest, "msg": msg})
            resp = socket.recv_json()
            if resp["status"] == "OK":
                print("Mensagem enviada com sucesso.")
            else:
                print("Erro ao enviar mensagem:", resp["msg"])
        elif opcao == "2":
            # Envio de mensagem para tópico
            topic = input("Tópico: ")
            msg = input("Mensagem: ")
            socket.send_json({"op": "top", "from": name, "topic": topic, "msg": msg})
            resp = socket.recv_json()
            if resp["status"] == "OK":
                print("Mensagem publicada com sucesso.")
            else:
                print("Erro ao publicar mensagem:", resp["msg"])
        else:
            print("Opção inválida.")

# Inicialização
sub_thread = threading.Thread(target=subscriber)
sub_thread.start()
client()

