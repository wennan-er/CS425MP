import InterNode_Comm
import threading
import time



import socket

MAXSIZE = 1024

class DateNode:
    def __init__(self, node_id, address):

        # id and address
        self.node_id = node_id
        self.address = address

        # membershipList and fileList

        # master elector related information stored in
        self.membershipList = MemberShipList()
        self.fileList       = FileList()
        self.electionList = dict()
        self.in_progress = False
        # TODO: Heartbeats Set up
        Heartbeat_process = HeartbeatSetUp()


        # TODO: Lock and critical information
        # master, lo
        self.masterAddress = (ip port)




    def PUT(self, localfilename, sdfsfilename):


        # A thread do the PUT work
        def threading_putwork(sdfsfilename):

            last_failed = False
            last_node = None

            # a client only connect to master
            master_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_client.connect(master_client)

            while last_failed:

                # e.g. "ASSIGN a.txt"
                msg = "ASSIGN "+sdfsfilename
                master_client.send(msg.encode())

                response = master_client.recv(MAXSIZE).decode()
                master_client.close()
                # can't PUT right now
                if response == "FILE IN USING":
                    print(response)
                    exit()
                # no node available
                elif response == "FAILED WRITE":
                    print(response)
                    exit()

                else:
                    peer_node_address = findNodeAddress(response)

                    # a client communicate with peer node to transfer
                    try:
                        peer_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        peer_client.connect(peer_node_address)

                        # send "SEND a.txt" to peer_client
                        msg = "SEND "+sdfsfilename
                        peer_client.send(msg.encode())

                    except:
                        print("can't connect peer node")

                    localfile = open(localfilename)

                    try:

                        content = localfile.read(MAXSIZE)
                        while content:
                                peer_client.send(content)
                                content = localfile.read(MAXSIZE)

                        localfile.close()
                        peer_client.close()

                        print("a thread to "+peer_node_address+"finished SENDING JOB")
                        last_failed = False

                        # send a CONFIRM to master
                        master_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        master_client.connect(master_client)

                        msg = "CONFIRM "+sdfsfilename + " " + response
                        master_client.send(msg.encode())

                        master_client.close()

                    except:
                        print("peer node "+peer_node_address+" is failed")
                        last_failed = True
                        last_node   = peer_node_address
                        print("now repeating")





        f = open(localfilename)
        if f is None:
            print("CAN'T FIND LOCAL FILE")
            return

        try:
            for i in range(4):
                threading_putworker(sdfsfilename)

        finally:
            f.close()



    def GET(self, localfilename, sdfsfilename):
        try:
            # a client only connect to master
            master_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_client.connect(master_client)

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
                f = open(localfilename)
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
            master_client.connect(master_client)

            # e.g. "FIND a.txt"
            msg = "PURGE " + sdfsfilename
            master_client.send(msg.encode())

            response = master_client.recv(MAXSIZE).decode()
            master_client.close()
        except:
            print("can't connect to master node")


    # given node_id, return (node_ip, node_port)
    def findNodeAddress(self, node_id):
