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


def updateElectionList(MyList,otherList):
    countMaster = 0
    masterID = 11
    resList = []
    myList = MyList.electionList
    if len(otherList) == 1:
        for line in otherList:
            [nodeId, otherTime, otherStatus] = line
            MyList.Master = nodeId
            # empty list will be evaluated in workerThread
            return []

    for line in otherList:
        [nodeId, otherTime, otherStatus] = line
        (myTime, myStatues) = myList.electionlist[nodeId]
        if nodeId == myList.id:
            continue
        if myTime < otherTime:
            myList.electionlist[nodeId] = (otherTime,otherStatus)
        (updateTime,updateStatus) = myList.electionlist[nodeId]
        if updateStatus != "None":
            countMaster += 1
            masterID = compareID(myList.id, masterID)
        resList.append([nodeId,updateTime,updateStatus])

    (myTime, myStatus) = myList.electionlist[myList.id]
    if myStatus != "None" and countMaster > 0:
        if compareID(myList.id, masterID) == masterID:
            myList.leaveElection(myList.id, datetime.datetime.now())
            countMaster -= 1
    resList.append([myList.id, datetime.datetime.now(), myStatus])
    if countMaster == 1:
        if masterID == 10:
            myList.Master = 'fa20-cs425-g29-'+str(masterID)+'.cs.illinois.edu'
        else:
            myList.Master = 'fa20-cs425-g29-'+'0'+str(masterID)+'.cs.illinois.edu'
        return [[myList.Master, datetime.datetime.now(), myList.Master]]
    MyList.plot_elect()
    return resList



