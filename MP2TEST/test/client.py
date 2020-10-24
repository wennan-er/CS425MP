import socket, pickle

class message:
    def __init__(self, type, address, port, data = 0):
        self.msgType = type
        self.msgAdd = address
        self.msgPort = port
        self.msgData = data


HOST = 'fa20-cs425-g29-01.cs.illinois.edu'
PORT = 8820
# Create a socket connection.

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    msg = message("ask", "11", "22")
    M = pickle.dumps(msg)
    s.sendall(M)
    s.close()

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    msg = message("ask", "11", "22")
    M = pickle.dumps(msg)
    s.sendall(M)
    s.close()

print ('Data Sent to Server')