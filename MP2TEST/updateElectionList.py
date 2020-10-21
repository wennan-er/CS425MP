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
# 'fa20-cs425-g29-01.cs.illinois.edu'

def compareID(myID, otherID):
    my = myID.split('-')[3]
    myid = int(my.split('.')[0])
    return min(myid, otherID)

"""
   some variables:
     1.countMaster: count how many masters in electionList,
                    when there is only one left, new master come out
     2.masterID: the smallest master in electionList
"""


def updateElectionList(myList,otherList):
    countMaster = 0
    masterID = 11
    resList = []
    if len(otherList) == 1:
        for line in otherList:
            [nodeId, otherStatus, otherTime] = line
            myList.Master = nodeId
            # empty list will be evaluated in workerThread
            return []

    for line in otherList:
        [nodeId, otherStatus, otherTime] = line
        (myStatus, myTime) = myList.list[nodeId]
        if nodeId == myList.id:
            continue
        if myTime < otherTime:
            myList.electionlist[nodeId] = (otherStatus, otherTime)
        (updateStatus, updateTime) = myList.electionlist[nodeId]
        if updateStatus:
            countMaster += 1
            masterID = compareID(myList.id, masterID)
        resList.append([nodeId, updateStatus, updateTime])

    (myStatus, myTime) = myList.electionlist[myList.id]
    if myStatus and countMaster > 0:
        if compareID(myList.id, masterID) == masterID:
            myList.leaveElection(myList.id, datetime.datetime.now())
            countMaster -= 1
    resList.append([myList.id, myStatus, myTime])
    if countMaster == 1:
        myList.Master = masterID
        return [[masterID, True, datetime.datetime.now()]]
    return resList



