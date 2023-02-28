import zmq, time
from constPS import * #-

context = zmq.Context()
s = context.socket(zmq.PUB)  # create a publisher socket
p = "tcp://" + HOST + ":" + PORT  # how and where to communicate
s.bind(p)  # bind socket to the address

while True:
    topic = input("Enter topic (or leave blank to publish to all subscribers): ")
    message = input("Enter message: ")
    if topic:
        s.send_multipart([str.encode(topic), str.encode(message)])
    else:
        s.send(str.encode(message))  # publish the current time
	
