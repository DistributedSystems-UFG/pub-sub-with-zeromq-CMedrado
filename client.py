import zmq.asyncio
import asyncio
import pickle
import constPS

class Client:
    def __init__(self, username):
        self.username = username
        self.registry = {}
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.REQ)

    async def register(self):
        self.socket.connect("tcp://%s:%s" % (constPS.REGISTRY_HOST, constPS.REGISTRY_PORT))
        msg_pack = ("register", self.username, constPS.CLIENT_HOST, constPS.CLIENT_PORT)
        marshaled_msg_pack = pickle.dumps(msg_pack)
        self.socket.send(marshaled_msg_pack)
        marshaled_reply = await self.socket.recv()
        reply = pickle.loads(marshaled_reply)
        if reply == "ACK":
            print("Registration successful")
        else:
            print("Registration failed")
        self.socket.close()

    async def get_registry(self):
        self.socket.connect("tcp://%s:%s" % (constPS.REGISTRY_HOST, constPS.REGISTRY_PORT))
        msg_pack = ("get_registry",)
        marshaled_msg_pack = pickle.dumps(msg_pack)
        self.socket.send(marshaled_msg_pack)
        marshaled_reply = await self.socket.recv()
        reply = pickle.loads(marshaled_reply)
        if reply:
            self.registry = reply
            print("Registry updated")
        else:
            print("Registry not available")
        self.socket.close()

    async def send_individual_msg(self, dest, msg):
        dest_addr = self.registry.get(dest)
        if dest_addr is None:
            print("Destination user not found")
            return
        client_sock = self.context.socket(zmq.REQ)
        dest_ip = dest_addr[0]
        dest_port = dest_addr[1]
        try:
            await client_sock.connect("tcp://%s:%s" % (dest_ip, dest_port))
        except:
            print("Destination user is down")
            client_sock.close()
            return
        msg_pack = ("user", dest, self.username, msg)
        marshaled_msg_pack = pickle.dumps(msg_pack)
        await client_sock.send(marshaled_msg_pack)
        marshaled_reply = await client_sock.recv()
        reply = pickle.loads(marshaled_reply)
        if reply == "ACK":
            print("Message sent successfully")
        else:
            print("Message not delivered")
        client_sock.close()

    def run(self):
        while True:
            choice = input("Enter action (register, update, individual, topic, exit): ")
            if choice == "register":
                asyncio.run(self.register())
            elif choice == "update":
                asyncio.run(self.get_registry())
            elif choice == "individual":
                dest = input("Enter destination username: ")
                msg = input("Enter message: ")
                asyncio.run(self.send_individual_msg(dest, msg))
            elif choice == "topic":
                asyncio.run(self.send_topic_msg())
            elif choice == "exit":
                break
            else:
                print("Invalid choice")
        self.context.term()

if __name__ == "__main__":
    username = input("Enter username: ")
    client = Client(username)
    client.run()
