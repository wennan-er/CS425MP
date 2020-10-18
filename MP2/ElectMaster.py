
"""
   Election rule1:
   1.A masterNode fails(say Node 1)
   2.Each DataNode will loop through its membershipList, send the elect message to current smallest Node(Node 2)
   3.When Node 2 received more than half of the messages, Node 2 will be elected as the new masterNode
   4.Then Node 2 will send confirm information to all DataNodes.
   5.Election Completed.

   Election Rule2: (use this)
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
        if self.Master == False && !self.in_progress:
            self.electionList = [node1:[True, time], node2: [True, time]]
            self.electionSenderQueue.put(self.electionList)
            self.in_progress = True
            electionReceiverThread()
        sleep(1)



def electionReceiverThread():
    while self.electionReceiverQueue not empty:
        pop
        finish = updateElectionList()
        if finish:
            sleep(3)
            broadcast new masterNode
        else:
            push to SenderQueue


def electionSenderThread():
    while self.electionSenderQueue not empty:
        for node in list:
            send ElectionList


def updateElectionList():



"""
   Each Node has an electionThread:
   1.send ReElect message to newMaster
   2.receive confirmation from newMaster
"""

def electionThread():
    now self.Master = NULL
    for node_id in membershipList:
        find node with smallest id as newMaster
    send "ReElect" to newMaster
    while(1):
        waiting for confirmation message from newMaster
        self.Master = newMaster
        break
"""
    Each Node has a listenElectionThread:
    1.each ReElect message will increment its global variable: electionCount
    2.after electionCount is more than #nodes/2, ReElect success
    3.broadcast newMaster to all other nodes in MembershipList

"""

# global variable
self.electionCount = 0
def listenElectionThread():
    while(1):
        self.electionCount++
        if self.electionCount > 1/2 * total:
            # change
            self.Master = self.node_id
            broadcast confirmation newMaster
            break


def






