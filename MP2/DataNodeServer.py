import socketserver
import socket
import threading
# import MembershipList
from random import shuffle
from collections import defaultdict
import sys

MAXSIZE = 1024


DATANODE_SERVER_PORT    = 8089
MASTERNODE_SERVER_PORT  = 8099

"""
    TEST MODE:
        node_id -> ('localhost', node_id)

    In real, it should be
        node_id -> (node_id, constant_server_port)

"""
def findNodeAddress(node_id):
    return ('localhost', node_id)
    # return (node_id, DATANODE_SERVER_PORT)



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

        4.  "CONFIRM [node_id] [filename]"
            sent by the PUT NODE, mark a confirmation of a replication of a.txt
            when 4 storage confirmed, mark this file successful uploaded, and write it into FileList

        5.  "REASSIGN [node_id] [filename]"
            similar to assign case, however, make this node_id as failed, then assign a new one

"""


class DataNodeServerHandler(socketserver.BaseRequestHandler):


    def __init__(self, request, client_address, server):
        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)
        return


    # TODO Check encode/decode case
    def handle(self):
        data = self.request.recv(MAXSIZE).decode()

        print("Receive data:{}".format(data))

        # praser
        # example: "ASSIGN a.txt"
        reply = ""
        # from_host is the hostname of the client.
        from_host = socket.gethostbyaddr(self.client_address[0])[0]

        splits = data.split(' ')

        # "GET a.txt"
        if splits[0] == "GET":
            self.GET(splits[1])

        # "PUT a.txt"
        elif splits[0] == "PUT":
            self.PUT(splits[1])

        # "SEND a.txt node_id"
        elif splits[0] == "SEND":
            self.SEND(splits[1], splits[2])



        # server handle "GFT sdfsfilename"
    def GET(self, sdfsfilename):

        # "r" - read mode
        sdfsfile = open(sdfsfilename, "rb")

        # read form sdfsfile, write into socket
        content = sdfsfile.read(MAXSIZE)
        while content:
            self.request.send(content)
            content = sdfsfile.read(MAXSIZE)

        sdfsfile.close()
        return



        # server handle "PUT sdfsfilename"
    def PUT(self, sdfsfilename):

        print("server start a put job, sdfsilename:" + sdfsfilename)

        # "w" - overwrite
        sdfsfile = open(sdfsfilename, "wb")


        print("open ")

        # read from socket, write into sdfs
        content = self.request.recv(MAXSIZE)
        while content:
            #print("Receive data:{}".format(content.decode()))

            sdfsfile.write(content)
            content = self.request.recv(MAXSIZE)

        sdfsfile.close()

        print("put job: " +sdfsfilename + "finished")

        return








        # MasterNode client send "SEND sdfsfilename node_id"
        # DateNode will send sdfsfile to Target DataNode
        #  - if successful, reply "BACKUP sdfsfilename node_id"
        #  - if Target DataNode failed, reply "FAIL_BACKUP sdfsfilename node_id"
        #       to master node  and finish this request
        # if this node failed, let MasterNode decide the next action
    def SEND(self, sdfsfilename, target_DataNode):

        peer_node_address = findNodeAddress(target_DataNode)

        sdfsfile = open(sdfsfilename, "rb")

        # a client communicate with peer node to transfer
        try:
            # ++++++ a peer client to target_Datanode   ++++++
            peer_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_client.connect(peer_node_address)

            # send "PUT a.txt" to peer_client
            msg = "PUT " + sdfsfilename
            peer_client.send(msg.encode())

            content = sdfsfile.read(MAXSIZE)
            while content:
                peer_client.send(content)
                content = sdfsfile.read(MAXSIZE)

            sdfsfile.close()
            peer_client.close()

            print("a thread to " + peer_node_address + "finished SENDING JOB")
            # ++++++        already back up             ++++++

            # send a confirm information to master
            msg = "BACKUP " + sdfsfilename + target_DataNode
            self.request.send(msg.encode())

        except:
            # backup failed, send an information to master
            msg = "FAIL_ACKUP " + sdfsfilename + target_DataNode
            self.request.send(msg.encode())
            
    def UPDATE(self, )

# Similar to lib example
class DataNodeServer(socketserver.TCPServer):

    # server_address = (localhost, port)
    def __init__(self, server_address, handler_class=DataNodeServerHandler):

        print("this server ip is: " + str(server_address[0]))
        print("this server port is: " + str(server_address[1]))
        socketserver.TCPServer.__init__(self, server_address, handler_class)
        return

    def server_activate(self):
        #self.logger.debug('server_activate')
        socketserver.TCPServer.server_activate(self)
        return

    def serve_forever(self):
        #self.logger.debug('waiting for request')

        print("serve is running")
        #self.logger.info('Handling requests, press <Ctrl-C> to quit')
        while True:
            self.handle_request()
        return

    def handle_request(self):
        #self.logger.debug('handle_request')
        return socketserver.TCPServer.handle_request(self)

    def verify_request(self, request, client_address):
        #self.logger.debug('verify_request(%s, %s)', request, client_address)
        return socketserver.TCPServer.verify_request(self, request, client_address)

    def process_request(self, request, client_address):
        #self.logger.debug('process_request(%s, %s)', request, client_address)
        return socketserver.TCPServer.process_request(self, request, client_address)

    def server_close(self):
        #self.logger.debug('server_close')
        return socketserver.TCPServer.server_close(self)

    def finish_request(self, request, client_address):
        #self.logger.debug('finish_request(%s, %s)', request, client_address)
        return socketserver.TCPServer.finish_request(self, request, client_address)

    def close_request(self, request_address):
        #self.logger.debug('close_request(%s)', request_address)
        return socketserver.TCPServer.close_request(self, request_address)

#
#
# if __name__ == '__main__':
#     # nope, don't test this server alone
#     # integrate this into DataNode and test together