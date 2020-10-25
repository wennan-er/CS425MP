import threading
import time
import sys
import DataNodeServer as DataNodeServer
import MasterServer as MasterServer
from MembershipList import MembershipList
from WorkerThread import updateMembershipList
import socket, pickle
import threading
from queue import Queue
import time
import datetime
from Util import Dict2List, List2Str, Str2List, randomChoose, message

import socket

MAXSIZE = 1024

DATANODE_PORT           = 8080          # not use yet
DATANODE_SERVER_PORT    = 8089          # for DataNode Server
MASTERNODE_SERVER_PORT  = 8099          # If I'm master
"""
    TEST MODE:
        node_id -> ('localhost', node_id)

    In real, it should be
        node_id -> (node_id, constant_server_port)

"""


class DateNode:
    def __init__(self, node_id):

        # identity
        self.node_id = node_id
        self.MyList = MembershipList(id=self.node_id)
        self.port = self.MyList.dic[node_id][0]
        self.intro = self.MyList.introducer_list

        # new variable for mp2
        self.port2 = self.MyList.dic[node_id][1]
        self.in_electionProgress = False  # indicate in election progress
        self.electionSenderQueue = Queue()
        self.electionReceiverQueue = Queue()
        self.master_thread = None


        # FOR TEST ('localhost', port)
        # self.address_ip     = address[0]
        # self.address_port   = int(address[1])

        # membershipList and fileList

        # master elector related information stored in
        #self.membershipList = MemberShipList()
        #self.fileList       = FileList()

        # real master address should be choose by a elector function
        ###### FOR TEST ONLY ######
        # self.master_address = ('localhost', MASTERNODE_SERVER_PORT)
        ###### FOR TEST ONLY ######

        ###### VARIABLES FROM MP1 ######
        # loss rate
        self.loss_rate = 0.1

        # isInGroup is an Event, default as False
        self.isInGroup = threading.Event()

        self.stillAlive = True

        # parameter
        self.sleepTime = 1

        # Boardcast Mode
        self.isGossip = True
        self.broadcastModeLock = threading.Lock()

        # creating the Sender Thread
        self.SenderList = []
        self.SenderQueue = Queue()
        threading_sender = threading.Thread(target=self.MySenderThread,
                                            args=(self.broadcastModeLock,))

        # creating the Receiver Thread
        self.ReceiverQueue = Queue()
        threading_receiver = threading.Thread(target=self.MyReceiverThread,
                                              )

        # increment self heartbeat
        threading_tiktok = threading.Thread(target=self.MyHeartThread,
                                            )

        # Worker Thread: compare received List and update self's List
        threading_worker = threading.Thread(target=self.MyWorkingThread,
                                            )

        # checker Thread: start check suspect node and label failure later
        threading_checker = threading.Thread(target=self.MyCheckerThread,
                                             )

        # election checker Thread: start check if there is no master node
        threading_checkMaster = threading.Thread(target=self.checkMasterThread,
                                                 )
        threading_electionSender = threading.Thread(target=self.electionSenderThread,
                                                    )
        threading_electionReceiver = threading.Thread(target=self.electionReceiverThread,
                                                      )
        ###### START ALL THE THREADS ######
        # but Receiver and Tiktok won't work until isInGroup
        threading_sender.start()
        threading_receiver.start()
        threading_tiktok.start()
        threading_worker.start()
        threading_checker.start()
        threading_checkMaster.start()
        threading_electionSender.start()
        threading_electionReceiver.start()

        # create a DataNode Server, let it keep running
        self.datanode_server = DataNodeServer.DataNodeServer(server_address= (self.node_id, DATANODE_SERVER_PORT))

        self.thread_server = threading.Thread(target= self.Maintain_server,
                                                 daemon= True)
        self.thread_server.start()

        # create a MasterNode Server, when a new master come out, start this server
        self.master_thread = None



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

                # "ls sdfsfilename"
                elif keyboard_cmd[0] == "ls":
                    (ops, sdfsfilename) = keyboard_cmd
                    self.LS(sdfsfilename = sdfsfilename)

                # "store"
                elif keyboard_cmd[0] == "store":
                    self.STORE()

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
                #         # CMD: KILL node_i

                elif keyboard_cmd[0] == "KILL":
                    self.stillAlive = False
                    sys.exit()

                # CMD: LEFT node_i
                elif keyboard_cmd[0] == "LEFT":
                    self.LeftAction()
                    # set isInGroup to False
                    self.isInGroup.clear()

                # CMD: JOIN
                elif keyboard_cmd[0] == "JOIN":
                    self.JoinAction()
                    # set isInGroup to True
                    self.isInGroup.set()

                else:
                    print("WRONG CMD, PLEASE RETRY")
            except:
                print("SOMETHING WRONG IN RUNNING THIS CMD")

    def Maintain_server(self):
        self.datanode_server.serve_forever()

    def set_new_master(self):
        self.master_thread = threading.Thread(target= self.create_master_and_run)
        self.master_thread.start()

    def kill_old_master(self):
        if self.master_thread != None:
            self.master_thread.kill()
            self.master_thread = None

    def create_master_and_run(self):
        masternode_server = MasterServer.MasterServer(
            server_address=(self.node_id, MASTERNODE_SERVER_PORT)
        )
        masternode_server.forver()

        # Receiver is a server:

    def MySenderThread(self, BroadcastModeLock):
        BUFFERSIZE = 4096

        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        while self.stillAlive:

            # Get the send List
            SendList = self.SenderQueue.get()
            # print("get a send job")
            if SendList:
                # find the curr BroadcastMode
                BroadcastModeLock.acquire()
                try:
                    curr_mode = self.isGossip
                    # print("Gossip") if self.isGossip else print("ALL2ALL")
                finally:
                    BroadcastModeLock.release()

                # candidateSet = list + introducer - self
                candidateSet = set()
                # candidateSet.union(set(self.MyList.list.keys()))
                # candidateSet.union(set(self.intro))
                for k in self.MyList.list.keys():
                    candidateSet.add(k)
                for k in self.intro:
                    candidateSet.add(k)
                candidateSet.discard(set(self.node_id))
                # print("candidate are:")
                # print(candidateSet)
                # SenderList depends on broadcast mode
                if curr_mode:
                    nodeIdList = randomChoose(list(candidateSet))
                else:
                    nodeIdList = list(candidateSet)

                # send part
                for nodeId in nodeIdList:
                    server_address = (nodeId, self.MyList.dic[nodeId][0])

                    SendString = List2Str(SendList)
                    # print("Sending String heartbeat: ", SendString)
                    try:
                        # Send data
                        sent = sock.sendto(SendString.encode(), server_address)
                    except:
                        print("Can't Send heartbeat")

    def MyReceiverThread(self):
        BUFFERSIZE = 1024

        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        # Bind the socket to the port
        server_address = (self.node_id, self.port)
        # print("Receiver Working with server_address", server_address)
        sock.bind(server_address)

        while self.stillAlive:
            # Only working if is in the group
            self.isInGroup.wait()

            # print("get a receive job")
            data, Sender = sock.recvfrom(BUFFERSIZE)
            if data:
                # print("just receive:", data)
                rec_str = data.decode('UTF-8')
                # Just for test
                rec_list = Str2List(rec_str)

                # print("received", rec_str)
                self.ReceiverQueue.put(rec_list)

    def MyHeartThread(self):
        while self.stillAlive:
            # Only working if is in the group
            self.isInGroup.wait()

            # sleep
            time.sleep(self.sleepTime)

            # update heartbeat in MyList
            new_heartbeat = datetime.datetime.now()
            self.MyList.update(self.node_id, new_heartbeat, "ACTIVE")

            # Create SenderList from MyList
            ListToSend = Dict2List(self.MyList.list)

            # send to SenderQueue
            self.SenderQueue.put(ListToSend)

    """
    Update and Sendout self Heartbeat and status as JOIN
    """

    def JoinAction(self):
        # update heartbeat in MyList as JOIN
        new_heartbeat = datetime.datetime.now()
        self.MyList.join(self.node_id, new_heartbeat, "JOIN")

        # Create SenderList from MyList
        ListToSend = Dict2List(self.MyList.list)

        # send to SenderQueue
        self.SenderQueue.put(ListToSend)

    """
    Update and Send out self Heartbeat and status as LEFT
    """

    def LeftAction(self):
        # update heartbeat in MyList as JOIN
        new_heartbeat = datetime.datetime.now()
        self.MyList.left(self.node_id, new_heartbeat, "LEFT")

        # Create SenderList from MyList
        ListToSend = Dict2List(self.MyList.list)

        # send to SenderQueue
        self.SenderQueue.put(ListToSend)

    def MyWorkingThread(self):
        while self.stillAlive:

            RecList = self.ReceiverQueue.get()

            # lock on
            if RecList:
                SendList = updateMembershipList(self.MyList, RecList, t_session=5, My_node_id=self.node_id)
            # lock off
            # self.SenderQueue.put(SendList)

    def MyCheckerThread(self, t_suspect=5, t_failed=10):

        # converse from int to datetime format
        t_suspect = datetime.timedelta(seconds=t_suspect)
        t_failed = datetime.timedelta(seconds=t_failed)

        while self.stillAlive:

            # Only working if is in the group
            self.isInGroup.wait()
            # print("checking")

            # judge based on curr time
            curr_time = datetime.datetime.now()

            for node_id in list(self.MyList.list.keys()):
                (heartbeat, statues) = self.MyList.list[node_id]
                pasted = curr_time - heartbeat

                # changed from FAIL to REMOVE
                if pasted > t_failed and statues in {"ACTIVE", "JOIN", "SUSPECT"}:
                    self.MyList.remove(node_id)

                # if already FAIL or SUSPECT or LEFT, do nothing
                elif pasted > t_suspect and statues in {"ACTIVE", "JOIN"}:
                    self.MyList.suspect(node_id)

            self.MyList.plot()
            time.sleep(self.sleepTime)

    def checkMasterThread(self):
        # sleep for join MembershipList
        time.sleep(3)
        while self.stillAlive:
            # Only working if is in the group
            self.isInGroup.wait()
            print("checking for master")
            time.sleep(0.5)

            # case1: the first time node enter group, ask for master
            if self.MyList.Master == "None" and not self.in_electionProgress:
                # first node enter the group, elect itself to be master
                if len(self.MyList.list) == 1:
                    # fist time start master, create masterServer
                    # TODO: start new masterServer
                    self.MyList.Master = self.node_id
                    self.set_new_master()
                else:
                    # loop through all nodes in membershipList, ask for master information
                    for node in self.MyList.list.keys():
                        if node == self.node_id:
                            continue
                        # message: type:ask destAddr:node, destPort:dic[node][1],data:null
                        mesg = message("ask", node, self.MyList.dic[node][1], self.node_id, self.MyList.dic[self.node_id][1])
                        print("send msg ask to:",node)
                        # put ask message into queue, senderThread will send them to dest
                        self.electionSenderQueue.put(mesg)

            # case2: when failure happens on Master, change self.Master to False
            if self.MyList.Master != "None" and self.MyList.Master not in self.MyList.list:
                self.MyList.Master = "None"
                self.in_electionProgress = True
                minMasterId = 11
                # loop through all nodes in membershipList, find the smallest one as the masternode
                for node in self.MyList.list.keys():
                    curId = int(node.split('-')[3].split('.')[0])
                    minMasterId = min(minMasterId, curId)
                    if minMasterId == 10:
                        electNodeId = 'fa20-cs425-g29-10.cs.illinois.edu'
                    else:
                        electNodeId = 'fa20-cs425-g29-0'+str(minMasterId)+'.cs.illinois.edu'
                # send election message to sender, destAddr: electNodeId
                mesg = message("election", electNodeId, self.MyList.dic[electNodeId][1],self.node_id, self.MyList.dic[self.node_id][1])
                self.electionSenderQueue.put(mesg)

            # case3:
            if self.MyList.Master != "None":
                # when first enter or a new smaller node join in, they will change to smaller master node
                minMasterId = 11
                for node in self.MyList.list.keys():
                    curId = int(node.split('-')[3].split('.')[0])
                    minMasterId = min(minMasterId, curId)
                    if minMasterId == 10:
                        electNodeId = 'fa20-cs425-g29-10.cs.illinois.edu'
                    else:
                        electNodeId = 'fa20-cs425-g29-0' + str(minMasterId) + '.cs.illinois.edu'
                if self.MyList.Master != electNodeId:
                    # if prev master is self, now change to other, kill self's master server
                    if self.MyList.Master == self.node_id:
                        self.kill_old_master()
                        #TODO: kill master
                    mesg = message("change", electNodeId, self.MyList.dic[electNodeId][1], self.node_id,
                                   self.MyList.dic[self.node_id][1])
                    self.electionSenderQueue.put(mesg)
                    self.MyList.Master = electNodeId
                print("current master is:", self.MyList.Master)
                self.in_electionProgress = False


    def electionSenderThread(self):
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        while self.stillAlive:
            electMsg = self.electionSenderQueue.get()
            server_address = (electMsg.destAddr, electMsg.destPort)
            data = pickle.dumps(electMsg)
            print('sending to:', electMsg.destAddr, '  port is:', electMsg.destPort)
            print("send msg :", electMsg.msgType)
            try:
                sent = sock.sendto(data, server_address)
            except:
                print("Can't Send message")



    def electionReceiverThread(self):
        BUFFERSIZE = 1024

        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        # Bind the socket to the port
        server_address = (self.node_id, self.port2)
        # print("Receiver Working with server_address", server_address)
        sock.bind(server_address)

        masterCount = 0
        while self.stillAlive:
            # print("I'm listening")
            msg_data, Sender = sock.recvfrom(BUFFERSIZE)

            data = pickle.loads(msg_data)
            if data.msgType == "ask":
                print("receive ask for master from",data.sourceAddr)
                if self.MyList.Master != "None":
                    replyMsg = message("reply ask", data.sourceAddr, data.sourcePort,self.node_id, self.MyList.dic[self.node_id][1], self.MyList.Master)
                    self.electionSenderQueue.put(replyMsg)

            elif data.msgType == "reply ask":
                self.MyList.Master = data.msgData

            elif data.msgType == "broadcast master":
                self.MyList.Master = data.msgData

            elif data.msgType == "change":
                if self.MyList.Master != self.node_id:
                    # TODO: start new masterServer
                    self.set_new_master()

            elif data.msgType == "election":
                masterCount += 1
                if masterCount >= 1/2*len(self.MyList.list):
                    self.MyList.Master = self.node_id
                    # TODO: start new masterServer
                    self.set_new_master()
                    self.setNewMaster = True
                    for node in self.MyList.list.keys():
                        broadMsg = message("broadcast master", node, self.MyList.dic[node][1],self.node_id, self.MyList.dic[self.node_id][1], self.MyList.Master)
                        self.electionSenderQueue.put(broadMsg)



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
                peer_node_address = self.get_peerserver_address(avail_node)
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
    # If no master, pass
    if self.MyList.Master == "None":
        pass
    # return ('fa20-cs425-g29-01.cs.illinois.edu', 9999)
    return (self.MyList.Master, MASTERNODE_SERVER_PORT)


# in real, node_id is 'fa20-cs425-g29-01.cs.illinois.edu'
# in test, node_id is the port '8080',

def get_peerserver_address(self, peer_node_id):
    # need to return server's address
    peerserver_address = (peer_node_id, DATANODE_SERVER_PORT)
    # peerserver_address = ('localhost', int(peer_node_id) + 9)

    return peerserver_address







if __name__ == "__main__":
    node_id = "fa20-cs425-g29-" + sys.argv[1] + ".cs.illinois.edu"
    print(f"Node name is : {node_id}")

    datanode = DateNode(node_id)

