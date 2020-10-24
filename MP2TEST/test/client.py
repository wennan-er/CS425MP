import socket, pickle

class message:
    def __init__(self, type, address, port, data = 0):
        self.msgType = type
        self.msgAdd = address
        self.msgPort = port
        self.msgData = data


HOST = 'localhost'
PORT = 8820
# Create a socket connection.
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))

# Create an instance of ProcessData() to send to server.
msg = message("ask","11","22")
# Pickle the object and send it to the server
M = pickle.dumps(msg)
s.send(M)

s.close()
print ('Data Sent to Server')