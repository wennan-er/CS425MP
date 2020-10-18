import socketserver
import socket
import threading

MAXSIZE = 1024


"""
        MasterNode operate as:
            1.  A extra process calling Master Server.
            2.  A daemon process checking FileList, send out each time
        
        All request are handled by MasterHandle, including the following case:
        
        1.  "ASSIGN [filename]"
            The first step of PUT
            Choose 4 node, add (key:filename, value: [nodes, ING/SUCC/FAIL])
            * pay attention to lock part.
        
        2.  "FIND [filename]"
            The first step of GET, check the FileList and 
                if can't find, return "404 NOT FOUND"
            * don't forget the lock
        
        3.  "PURGE [filename]"
            check the fileList, find all Storage Node and tell to "DELETE a.txt"
            * lock, lock, lock
        
        4.  "CONFIRM PUT [node_id] [filename]"
            sent by the PUT NODE, mark a confirmation of a replication of a.txt
            when 4 storage confirmed, mark this file successful uploaded, and write it into FileList
            
        5.  "REASSIGN [node_id] [filename]"
            similar to assign case, however, make this node_id as failed, then assign a new one

"""
class MasterHandler(socketserver.BaseRequestHandler):


    def __init__(self):
        self.fileList =

    # TODO: start from here
    def handle(self):
        self.data = self.request.recv(MAXSIZE)

        # praser
        # example: "ASSIGN a.txt"
        reply = ""
        requester = (client_address, clientport)

        if self.data[:6] == "ASSIGN":
            filename = self.data[7:]

            # check availability for this file
            if not self.acquire_file_lock(filename, requester):
                # tell the requesting node, this file is using right now.
                reply = "FILE IN USING"

            # if a new
            else:

            new_StorageNode = self.PUT_assign(filename)

        if self.data[:]

    # TODO

    # acquire the lock by filename, return True if 1. gained, or 2. requester is the locker user, otherwise False
    def acquire_file_lock(self, filename, requester):

        if lock_filename in using and user == requester:
            return True
        pass

    # should already acquire the lock
    def unlock_file_lock(self, filename):
        pass

    def ASSIGN(self, filename):
        pass

    def FIND(self, filename):
        pass

    def PURGE(self, filename):
        pass

    def CONFIRM(self, node_id, filename):
        pass

    def REASSIGN(self, filename):
        pass

# Similar to lib example
class MasterServer(socketserver.TCPServer):

    # server_address = (localhost, port)
    def __init__(self, server_address, handler_class= MasterHandler):
        socketserver.BaseRequestHandler.__init__(self, server_address, handler_class)
        return


    def server_activate(self):
        socketserver.TCPServer.server_activate(self)

    def serve_forever(self, poll_interval=0.5):
        while True:
            self.handle_request()
        return

    def handle_request(self):
        return socketserver.TCPServer.handle_request(self)

    def verify_request(self, request, client_address):
        return socketserver.TCPServer.verify_request(self, request, client_address)

    def process_request(self, request, client_address):
        return socketserver.TCPServer.process_request(self, request, client_address)

    def server_close(self):
        return socketserver.TCPServer.server_close(self)

    def finish_request(self, request, client_address):
        return socketserver.TCPServer.finish_request(self, request, client_address)

    def close_request(self, request):
        return socketserver.TCPServer.close_request(self, request)




