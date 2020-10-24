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
    # record the smallest masterID in election
    minMasterID = 11
    resList = []
    myList = MyList.electionList

    for line in otherList:
        [nodeId, otherTime, otherStatus] = line
        (myTime, myStatues) = myList.electionlist[nodeId]
        if nodeId == myList.id:
            continue
        if myTime < otherTime:
            myList.electionlist[nodeId] = (otherTime,otherStatus)
        (updateTime,updateStatus) = myList.electionlist[nodeId]
        if updateStatus != "None":
            minMasterID = compareID(myList.id, minMasterID)
        resList.append([nodeId,updateTime,updateStatus])

    (myTime, myStatus) = myList.electionlist[myList.id]
    if myStatus != "None":
        if compareID(myList.id, minMasterID) == minMasterID:
            myList.leaveElection(myList.id, datetime.datetime.now())
            myStatus = "None"
    resList.append([myList.id, datetime.datetime.now(), myStatus])
    MyList.plot_elect()
    return resList



