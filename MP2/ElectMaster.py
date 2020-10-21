import datetime

"""
   Election Rule:
   1.Each DataNode found there are no MasterNode in system,
     and will update electionList as everyone is MasterNode
   2.Each DataNode will broadcast their electionList to all DataNodes,
     update their electionList based on
       1.change self master state to False if there is smaller candidate node in list
         keep self master sate False
       2.update recent electionList except self

"""
"""
    checkMaster runs every 1 second to check is master fails
    
"""

def checkMasterThread(self):
    while self.stillAlive:
        time.sleep(0.5)

        # case: when failure happens on Master, change self.Master to False
        if self.Master != False and self.Master not in self.MyList.list:
            self.Master = False
        # first enter election progress when self.Master become False
        if self.Master == False and not self.in_progress:
            elecList = []
            self.electionList = dict()
            for nodeID in self.MyList.list:
                elecList.append([nodeID,True, datetime.datetime.now()])
                self.electionList[nodeID] = (True, datetime.datetime.now())
            self.electionSenderQueue.put(elecList)
            self.in_progress = True
        # new master come out
        if self.Master:
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
            print("Sending String: ", SendString)
            try:
                sent = sock.sendto(SendString.encode(), server_address)
            except:
                print("Can't Send")

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
            print("just receive:", data)
            rec_str = data.decode('UTF-8')
            # Just for test
            rec_list = Str2List(rec_str)

            # print("received", rec_str)
            self.electionReceiverQueue.put(rec_list)

"""
   workerThread read from electionReceiverQueue
   pass electionList to update function
   
"""

def elctionWorkerThread(self):
    while self.stillAlive and self.Master == NULL:
        otherList = self.electionReceiverQueue.get()
        if otherList:
            newList = updateElectionList(otherList)
            if not newList:
                break
            else:
                self.electionSenderQueue.put(newList)

"""
   some variables:
     1.countMaster: count how many masters in electionList,
                    when there is only one left, new master come out
     2.masterID: the smallest master in electionList
"""


def updateElectionList(otherList):
    countMaster = 0
    masterID = 11
    resList = []
    if len(otherList) == 1:
        for line in otherList:
            [nodeId, otherStatus, otherTime] = line
            self.Master = nodeId
            # empty list will be evaluated in workerThread
            return []

    for line in otherList:
        [nodeId, otherStatus, otherTime] = line
        (myStatus, myTime) = self.electionList[nodeId]
        if nodeId == self.node_id:
            continue
        if myTime < otherTime:
            self.electionList[nodeId] = (otherStatus, otherTime)
        (updateStatus, updateTime) = self.electionList[nodeId]
        if updateStatus:
            countMaster += 1
            masterID = compareID(key, masterID)
        resList.append([nodeId, updateStatus, updateTime])

    (myStatus, myTime) = self.electionList[self.node_id]
    if myStatus and countMaster > 0:
        if compareID(self.node_id, masterID) == masterID:
            self.electionList[self.node_id] = (False, myTime)
            countMaster -= 1
    resList.append([self.node_id, myStatus, myTime])
    if countMaster == 1:
        self.Master = masterID
        return [[masterID, True, datetime.datetime.now()]]
    return resList


# 'fa20-cs425-g29-01.cs.illinois.edu'

def compareID(myID, otherID):
    my = myID.split('-')[3]
    myid = int(my.split('.')[0])
    return min(myid, otherID)
