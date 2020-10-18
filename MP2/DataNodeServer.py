
import socketserver
import socket
import InterNode_Comm
MAXSIZE = 1024

"""
    This DataNodeServer should help the server inside each DataNode in follow request:
        1. a PUT request from other DataNode
        2. a GET request from other DataNode
        3. a DELETE request from MasterNode
"""
class DataNodeServerHandler(socketserver.BaseRequestHandler):


    def __init__(self):
        pass


    def handle(self):
        self.data = self.request.recv(MAXSIZE)

        # praser
        # example: "PUT a.txt/n"
        if self.data[:3] == "PUT":
            filename = self.data[4:]

            # perhaps a new thread to handle this one
            InterNode_Comm.readSocket_writeLocal(socket, filename)

        # "GET a.txt/n"
        elif self.data[:3] == "GET":
            filename = self.data[4:]

            # check file exists, otherwise reply with error code

            # perhaps a new thread to handle this one
            InterNode_Comm.readLocal_writeSocket(socket, filename)

        #  01234567890
        # "DELETE a.txt"
        elif self.data[:6] == "DELETE":
            filename = self.data[7:]

            # check file exists, if exists, delete

            # reply with "CONFIRM DELETE a.txt"

