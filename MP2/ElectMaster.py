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
def checkMaster():
    while True:
        if self.Master not in self.MyList.list:
            self.Master = False
        if self.Master == False && !self.in_progress:
            for keyID in self.list:
                self.electionList[keyID] = (True,datetime.datetime.now())
            self.electionSenderQueue.put(self.electionList)
            self.in_progress = True
            electionReceiverThread()
        if self.Master == True:
            self.in_progress = False
        sleep(1)



def electionReceiverThread():
    BUFFERSIZE = 1024
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET,
                         socket.SOCK_DGRAM)
    # Bind the socket to the port
    server_address = (self.node_id, self.port)
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


def workerThread():
    while self.stillAlive and self.Master == NULL:
        otherList = self.electionReceiverQueue.get()
        if otherList:
            newList = updateElectionList(otherList)
            if not newList:
               break
            else:
                self.electionSenderQueue.put(newList)


def electionSenderThread():
    sock = socket.socket(socket.AF_INET,
                         socket.SOCK_DGRAM)
    while self.stillAlive:
        electionList = self.electionSenderQueue.get()

        # send part
        for nodeId in electionList:
            server_address = (nodeId, self.MyList.dic[nodeId])
            SendString = List2Str(electionList)
            print("Sending String: ", SendString)
            try:
                sent = sock.sendto(SendString.encode(), server_address)
            except:
                print("Can't Send")


def updateElectionList(otherList):
    countMaster = 0
    masterID = 11
    resList = []
    if len(otherList) == 1:
        for line in otherList:
            [nodeId, otherStatus, otherTime] = line
            self.Master = nodeId
            return []

    for line in otherList:
        [nodeId, otherStatus, otherTime] = line
        (myStatus,myTime) = self.electionList[nodeId]
        if nodeId == self.node_id:
            continue
        if myTime < otherTime:
            self.electionList[nodeId] = (otherStatus,otherTime)
        (updateStatus, updateTime) = self.electionList[nodeId]
        if updateStatus:
            countMaster += 1
            masterID = compareID(key,masterID)
        resList.append([nodeId,updateStatus, updateTime])

    (myStatus, myTime) = self.electionList[self.node_id]
    if myStatus and countMaster > 0:
        if compareID(self.node_id,masterID) == masterID:
            self.electionList[self.node_id] = (False, myTime)
            countMaster -= 1
    resList.append([self.node_id, myStatus, myTime])
    if countMaster == 1:
        self.Master = masterID
        return [[masterID, True, datetime.datetime.now()]]
    return resList

# 'fa20-cs425-g29-01.cs.illinois.edu'

def compareID(myID,otherID):
    my = myID.split('-')[3]
    myid = int(my.split('.')[0])
    return min(myid,otherID)







