import socket, pickle
import time
import random

class message:
    def __init__(self, type, address, port, data = 0):
        self.msgType = type
        self.msgAdd = address
        self.msgPort = port
        self.msgData = data

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# Bind the socket to the port
server_address = ('127.0.0.1',8820)
# print("Receiver Working with server_address", server_address)
sock.bind(server_address)
sock.listen(1)

while True:
    print("Waiting for commands")
    (conn, client_address) = sock.accept()
    print ('Connected by', client_address)
    client_data = conn.recv(1024)
    d = pickle.loads(client_data)
    conn.close()
    print("type is",d.msgType)


sock.close()