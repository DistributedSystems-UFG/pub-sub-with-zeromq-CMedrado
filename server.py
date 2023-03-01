import zmq.asyncio
import asyncio
import pickle
import constPS

class ChatServer:
    def __init__(self):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://%s:%s" % (constPS.CHAT_SERVER_HOST, constPS.CHAT_SERVER_PORT))
        self.registry = {}

    async def handle_msg(self, msg_pack):
        dest_type = msg_pack[0]
        if dest_type == "user":
            await self.handle_individual_msg(msg_pack)
        elif dest_type == "topic":
            await self.handle_topic_msg(msg_pack)

    async def handle_individual_msg(self, msg_pack):
        dest = msg_pack[1]
        src = msg_pack[2]
        msg = msg_pack[3]
        # Verifica se o destinatário está registrado no servidor
        try:
            dest_addr = self.registry[dest]
        except:
            reply = ("ACK", "Destination user not found")
            marshaled_reply = pickle.dumps(reply)
            await self.socket.send(marshaled_reply)
            return
        # Cria uma conexão com o cliente destinatário
        client_sock = self.context.socket(zmq.REQ)
        dest_ip = dest_addr[0]
        dest_port = dest_addr[1]
        # Tenta se conectar ao cliente destinatário
        try:
            client_sock.connect("tcp://%s:%s" % (dest_ip, dest_port))
        except:
            reply = ("ACK", "Destination user is down")
            marshaled_reply = pickle.dumps(reply)
            await self.socket.send(marshaled_reply)
            client_sock.close()
            return
        # Envia a mensagem para o cliente destinatário e aguarda um ACK
        msg_pack = (src, msg)
        marshaled_msg_pack = pickle.dumps(msg_pack)
        client_sock.send(marshaled_msg_pack)
        marshaled_reply = await client_sock.recv()
        reply = pickle.loads(marshaled_reply)
        if reply != "ACK":
            reply = ("ACK", "Destination user did not receive message properly")
        else:
            reply = ("ACK", "Message sent successfully")
        marshaled_reply = pickle.dumps(reply)
        await self.socket.send(marshaled_reply)
        client_sock.close()

        async def run(self):
            while True:
                marshaled_msg_pack = await self.socket.recv()
                msg_pack = pickle.loads(marshaled_msg_pack)
                if msg_pack[0] == "register":
                    self.registry[msg_pack[1]] = (msg_pack[2], msg_pack[3])
                    reply = "ACK"
                    marshaled_reply = pickle.dumps(reply)
                    await self.socket.send(marshaled_reply)
                elif msg_pack[0] == "get_registry":
                    marshaled_reply = pickle.dumps(self.registry)
                    await self.socket.send(marshaled_reply)
                else:
                    await self.handle_msg(msg_pack)

    if __name__ == "__main__":
        chat_server = ChatServer()
        asyncio.run(chat_server.run())
