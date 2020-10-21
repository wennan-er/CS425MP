import sys

from MembershipList import MembershipList
from WorkerThread import updateMembershipList
import socket
import threading
from queue import Queue
import time
import datetime
from Util import Dict2List, List2Str, Str2List, randomChoose
from updateElectionList import updateElectionList

# In test
class Node:
    def __init__(self, node_id):

        # identity
        self.node_id = node_id
        self.MyList = MembershipList(id=self.node_id)
        self.port = self.MyList.dic[node_id][0]

        # new variable for mp2
        self.port2 = self.MyList.dic[node_id][1]
        self.in_progress = False  # indicate in election progress
        self.electionSenderQueue = Queue()
        self.electionReceiverQueue = Queue()


        self.intro = self.MyList.introducer_list

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
        threading_electionWorker = threading.Thread(target=self.elctionWorkerThread,
                                                      )




        # start all the threads
        # but Receiver and Tiktok won't work until isInGroup
        threading_sender.start()
        threading_receiver.start()
        threading_tiktok.start()
        threading_worker.start()
        threading_checker.start()
        threading_checkMaster.start()
        threading_electionSender.start()
        threading_electionReceiver.start()
        threading_electionWorker.start()

        self.MyKeyboardListener(self.node_id, self.MyList,
                                self.broadcastModeLock,
                                self.SenderQueue)

    # Listen to Keyboard, and control
    def MyKeyboardListener(self, node_id, MyList, BroadcastModeLock, SenderQueue):

        while True:

            # Read from the KeyBoard Input

            CMD = input("Waiting for CMD\n")

            # parser the CMD:
            try:

                # CMD: show MyNode_d
                if CMD == "show MyID":
                    print("My node_id is: " + node_id)

                # CMD: show MyList
                elif CMD == "show MyList":
                    print(MyList)

                # CMD: switch broadcast mode to ALL2ALL
                elif CMD == "switch to ALL2ALL":
                    BroadcastModeLock.acquire()
                    try:
                        self.isGossip = False
                    finally:
                        BroadcastModeLock.release()

                # CMD: switch broadcast mode to GOSSIP
                elif CMD == "switch to GOSSIP":
                    BroadcastModeLock.acquire()
                    try:
                        self.isGossip = True
                    finally:
                        BroadcastModeLock.release()

                # CMD: show the current broadcast
                elif CMD == "show curr_mode":
                    BroadcastModeLock.acquire()
                    try:
                        print("Gossip") if self.isGossip else print("ALL2ALL")
                    finally:
                        BroadcastModeLock.release()



                # CMD: KILL node_i
                elif CMD == "KILL":
                    self.stillAlive = False
                    sys.exit()

                # CMD: LEFT node_i
                elif CMD == "LEFT":
                    self.LeftAction()
                    # set isInGroup to False
                    self.isInGroup.clear()

                # CMD: JOIN
                elif CMD == "JOIN":
                    self.JoinAction()
                    # set isInGroup to True
                    self.isInGroup.set()

                else:
                    print("Can't read. Try another command")
            except:
                print("Something wrong, this machine may failed")

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
                    server_address = (nodeId, self.MyList.dic[nodeId])

                    SendString = List2Str(SendList)
                    print("Sending String heartbeat: ", SendString)
                    try:
                        # Send data
                        sent = sock.sendto(SendString.encode(), server_address)
                    except:
                        print("Can't Send heartbeat")

                    # finally:
                    #     print("send finish")

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
                print("just receive:", data)
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
            print("checking")

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
        while self.stillAlive:
            # Only working if is in the group
            self.isInGroup.wait()
            print("checking for master")
            time.sleep(0.5)

            # case: when failure happens on Master, change self.Master to False
            if self.MyList.Master != False and self.MyList.Master not in self.MyList.list:
                self.MyList.Master = False
            # first enter election progress when self.Master become False
            if self.MyList.Master == False and not self.in_progress:
                elecList = []
                self.MyList.electionList = dict()
                for nodeID in self.MyList.list:
                    elecList.append([nodeID, True, datetime.datetime.now()])
                    self.MyList.electionList[nodeID] = (True, datetime.datetime.now())
                self.electionSenderQueue.put(elecList)
                self.in_progress = True
            # new master come out
            if self.MyList.Master:
                print("new master is:",self.MyList.Master)
                self.in_progress = False
            time.sleep(0.5)

    def electionSenderThread(self):
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        while self.stillAlive:
            electionList = self.electionSenderQueue.get()

            # send part
            for line in electionList:
                [nodeId, statues, time] = line
                server_address = (nodeId, self.MyList.dic[nodeId][1])
                SendString = List2Str(electionList)
                print("Sending String election: ", SendString)
                try:
                    sent = sock.sendto(SendString.encode(), server_address)
                except:
                    print("Can't Send election")

    def electionReceiverThread(self):
        BUFFERSIZE = 1024
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        # Bind the socket to the port
        server_address = (self.node_id, self.port2)
        # print("Receiver Working with server_address", server_address)
        sock.bind(server_address)

        while self.stillAlive:

            data, Sender = sock.recvfrom(BUFFERSIZE)
            if data:
                print("just receive election:", data)
                rec_str = data.decode('UTF-8')
                # Just for test
                rec_list = Str2List(rec_str)

                # print("received", rec_str)
                self.electionReceiverQueue.put(rec_list)

    def elctionWorkerThread(self):
        while self.stillAlive and self.MyList.Master == False:
            otherList = self.electionReceiverQueue.get()
            if otherList:
                newList = updateElectionList(self.MyList.electionList,otherList)
                if not newList:
                    break
                else:
                    self.electionSenderQueue.put(newList)


if __name__ == "__main__":
    node_id = "fa20-cs425-g29-" + sys.argv[1] + ".cs.illinois.edu"
    print(f"Node name is : {node_id}")
    Node(node_id)
