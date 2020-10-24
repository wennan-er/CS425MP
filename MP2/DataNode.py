import threading
import time
import sys
import DataNodeServer as DataNodeServer


import socket

MAXSIZE = 1024

DATANODE_PORT           = 8080
#DATANODE_SERVER_PORT    = 8089
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

class DateNode:
    def __init__(self, node_id, address):

        # id and address
        self.node_id = node_id


        # FOR TEST ('localhost', port)
        self.address_ip     = address[0]
        self.address_port   = int(address[1])

        # membershipList and fileList

        # master elector related information stored in
        #self.membershipList = MemberShipList()
        #self.fileList       = FileList()
        #self.electionList = dict()



        # real master address should be choose by a elector function
        ###### FOR TEST ONLY ######
        self.master_address = ('localhost', MASTERNODE_SERVER_PORT)
        ###### FOR TEST ONLY ######


        # create a DataNode Server, let it keep running
        self.datanode_server = DataNodeServer.DataNodeServer(server_address= (self.address_ip, self.address_port+9))

        self.thread_server  = threading.Thread(target= self.Maintain_server,
                                                 daemon= True)
        self.thread_server.start()



        self.keyboard_listener = self.Keyboard_Listener(BroadcastModeLock= None, SenderQueue= None)


    def Keyboard_Listener(self, BroadcastModeLock, SenderQueue):

        print("start keyboard listening\n")

        while True:

            # Read from the KeyBoard Input

            keyboard_cmd = input("Waiting for CMD\n")
            keyboard_cmd = keyboard_cmd.split(' ')

            # parser the keyboard_cmd:
            try:

                """
                ops for MP2
                """

                # "put localfilename sdfsfilename"
                if keyboard_cmd[0] == "put":

                    (ops, localfilename, sdfsfilename) = keyboard_cmd
                    print("running " + ops + "cmd")
                    self.PUT(localfilename= localfilename, sdfsfilename= sdfsfilename)

                # "get sdfsfilename localfilename"
                elif keyboard_cmd[0] == "get":
                    (ops, sdfsfilename, localfilename) = keyboard_cmd
                    self.GET(localfilename= localfilename, sdfsfilename=sdfsfilename)

                # "delete sdfsfilename"
                elif keyboard_cmd[0] == "delete":
                    (ops, sdfsfilename) = keyboard_cmd
                    self.DELETE(sdfsfilename= sdfsfilename)


                else:
                    print("WRONG CMD, PLEASE RETRY")

                # # "ls sdfsfilename"
                # elif keyboard_cmd[0] == "ls":
                #     (ops, sdfsfilename) = keyboard_cmd
                #     self.LS(sdfsfilename = sdfsfilename)
                #
                # # "store"
                # elif keyboard_cmd[0] == "store":
                #     self.STORE()

                # # CMD: show MyNode_d
                # if CMD == "show MyID":
                #     print("My node_id is: " + self.node_id)
                #
                # # CMD: show MyList
                # elif CMD == "show MyList":
                #     print(self.MyList)
                #
                # # CMD: switch broadcast mode to ALL2ALL
                # elif CMD == "switch to ALL2ALL":
                #     BroadcastModeLock.acquire()
                #     try:
                #         self.isGossip = False
                #     finally:
                #         BroadcastModeLock.release()
                #
                # # CMD: switch broadcast mode to GOSSIP
                # elif CMD == "switch to GOSSIP":
                #     BroadcastModeLock.acquire()
                #     try:
                #         self.isGossip = True
                #     finally:
                #         BroadcastModeLock.release()
                #
                # # CMD: show the current broadcast
                # elif CMD == "show curr_mode":
                #     BroadcastModeLock.acquire()
                #     try:
                #         print("Gossip") if self.isGossip else print("ALL2ALL")
                #     finally:
                #         BroadcastModeLock.release()



                """
                MP1:    KILL
                        LEFT
                        JOIN
                """

                # # CMD: KILL node_i
                # elif keyboard_cmd[0] == "KILL":
                #     self.stillAlive = False
                #     sys.exit()
                #
                # # CMD: LEFT node_i
                # elif keyboard_cmd[0] == "LEFT":
                #     self.LeftAction()
                #     # set isInGroup to False
                #     self.isInGroup.clear()
                #
                # # CMD: JOIN
                # elif keyboard_cmd[0] == "JOIN":
                #     self.JoinAction()
                #     # set isInGroup to True
                #     self.isInGroup.set()





            except:
                print("SOMETHING WRONG IN RUNNING THIS CMD")




    def Maintain_server(self):
        self.datanode_server.serve_forever()



    def PUT(self, localfilename, sdfsfilename):
        # A thread do the PUT work
        def threading_putwork(sdfsfilename):

            print("a thread work has begin working")

            last_failed = True
            last_node = None



            while last_failed:

                # e.g. "ASSIGN a.txt"
                msg = "ASSIGN "+sdfsfilename


                response = self.request_from_master(msg)


                # can't PUT right now
                if response == "FILE IN USING":
                    print(response)
                    exit()
                # no node available
                elif response == "FAILED WRITE":
                    print(response)
                    exit()

                else:
                    # START PEER TRANSFER

                    peer_server_address = self.get_peerserver_address(response)
                    print("peer_server_ip: "    + peer_server_address[0])
                    print("peer_server_port: "  + str(peer_server_address[1]))
                    try:
                        peer_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        peer_client.connect(peer_server_address)

                        # send "PUT a.txt" to peer_client
                        msg = "PUT "+sdfsfilename
                        peer_client.send(msg.encode())

                    except:
                        print("can't connect peer node")

                    localfile = open(localfilename, "rb")

                    # try:
                    #
                    #     print("connect peer node " + response + " start trans")
                    #
                    #     content = localfile.read(MAXSIZE)
                    #     while content:
                    #             print("Send data:{}".format(content).encode())
                    #             peer_client.send(content)
                    #             content = localfile.read(MAXSIZE)
                    #
                    #     localfile.close()
                    #     peer_client.close()
                    #
                    #     print("a thread to "+ response +" finished SENDING JOB")
                    #     last_failed = False
                    #
                    #     # confirm with master
                    #     msg = "CONFIRM "+sdfsfilename + " " + response
                    #     self.request_from_master(msg)
                    #
                    # except:
                    #     print("peer node "+ response +" is failed")
                    #     last_failed = True
                    #     print("now repeating")

                    print("connect peer node " + response + " start trans")

                    content = localfile.read(MAXSIZE)
                    while content:
                        print("Send data:{}".format(content).encode())
                        peer_client.send(content)
                        content = localfile.read(MAXSIZE)

                    localfile.close()
                    peer_client.close()

                    print("a thread to " + response + " finished SENDING JOB")
                    last_failed = False

                    # "CONFIRM [node_id] [filename]"
                    msg = "CONFIRM " + response + " " + sdfsfilename
                    self.request_from_master(msg)





        f = open(localfilename, "rb")
        if f is None:
            print("CAN'T FIND LOCAL FILE")
            return

        # create a pool to control the thread
        thread_pool = []
        try:
            for i in range(1):
                thread_putworker = threading.Thread(target= threading_putwork,
                                                    args=(sdfsfilename,))
                thread_putworker.start()

                thread_pool.append(thread_putworker)

        finally:
            f.close()

    def GET(self, localfilename, sdfsfilename):
        try:
            # a client only connect to master
            master_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_client.connect(self.get_masterserver_address())

            # e.g. "FIND a.txt"
            msg = "FIND " + sdfsfilename
            master_client.send(msg.encode())

            response = master_client.recv(MAXSIZE).decode()
            master_client.close()
        except:
            print("can't connect to master node")

        if response == "FILE NOT FOUND OR NOT READABLE":
            print(response)
            return
        avail_node_list = response.split(',')


        for avail_node in avail_node_list:
            # last node is "", which meaning failed
            if avail_node is None:
                # arrive here means all nodes failed.
                print("all avail nodes are FAILED")
                return

            try:
                # create a client
                peer_node_address = self.findNodeAddress(avail_node)
                peer_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_client.connect(peer_node_address)


                # peer_client
                msg = "GET "+sdfsfilename
                peer_client.send(msg.encode())

                # write to local file
                f = open(localfilename, "wb")
                content = peer_client.recv(MAXSIZE)
                while content:
                    f.write(content)
                    content = peer_client.recv(MAXSIZE)

                f.close()
                peer_client.close()
            except:
                print("one peer node" + avail_node + "failed")
                print("NOW loop for next")

    def DELETE(self, sdfsfilename):
        try:
            # a client only connect to master
            master_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_client.connect(self.get_masterserver_address())

            # e.g. "PURGE a.txt"
            msg = "PURGE " + sdfsfilename
            master_client.send(msg.encode())
            response = master_client.recv(MAXSIZE).decode()
            master_client.close()
        except:
            print("can't connect to master node")

    # a temp client talk to master
    def request_from_master(self, msg):
        # a client only connect to master
        master_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_client.connect(self.get_masterserver_address())

        print("sending to master: " + msg)
        master_client.send(msg.encode())

        response = master_client.recv(MAXSIZE).decode()
        print("receive from master: " + response)
        master_client.close()

        return response




    # in real, this should wait for the master_address if there is no available one
    def get_masterserver_address(self):

        if self.master_address is None:
            pass

        address = ('fa20-cs425-g29-01.cs.illinois.edu', 9999)
        return self.master_address

    # in real, node_id is 'fa20-cs425-g29-01.cs.illinois.edu'
    # in test, node_id is the port '8080',

    def get_peerserver_address(self, peer_node_id):
        # need to return server's address
        peerserver_address = ('localhost', int(peer_node_id) + 9)

        return peerserver_address



if __name__ == "__main__":

    port = sys.argv[1]

    address = ('localhost', port)
    print("this datanode ip is: "+ address[0])
    print("this datanode port is: " + address[1])
    datanode = DateNode("test", address)

