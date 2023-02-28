import zmq
from constPS import HOST, PORT

context = zmq.Context()
s = context.socket(zmq.SUB)  # create a subscriber socket
p = "tcp://" + HOST + ":" + PORT  # how and where to communicate
s.connect(p)  # connect socket to the address
s.setsockopt_string(zmq.SUBSCRIBE, "")  # subscribe to all topics

while True:
    message = s.recv_multipart()
    print(f"Received {message[1].decode()} from topic {message[0].decode()}")
