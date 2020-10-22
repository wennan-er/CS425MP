
import logging
import datetime


"""
self.list[node_id] = tuple(heartbeat, status)
"""





class MembershipList:
    def __init__(self,id):
        self.list = dict()
        self.electionList = dict()
        self.id = id
        self.Master = "None"
        machine_dic = {'fa20-cs425-g29-01.cs.illinois.edu': [2156,2256],
                       'fa20-cs425-g29-02.cs.illinois.edu': [2157,2257],
                       'fa20-cs425-g29-03.cs.illinois.edu': [2158,2258],
                       'fa20-cs425-g29-04.cs.illinois.edu': [2159,2259],
                       'fa20-cs425-g29-05.cs.illinois.edu': [2160,2260],
                       'fa20-cs425-g29-06.cs.illinois.edu': [2161,2261],
                       'fa20-cs425-g29-07.cs.illinois.edu': [2162,2262],
                       'fa20-cs425-g29-08.cs.illinois.edu': [2163,2263],
                       'fa20-cs425-g29-09.cs.illinois.edu': [2164,2264],
                       'fa20-cs425-g29-10.cs.illinois.edu': [2165,2265]}
        self.dic = machine_dic
        introducer_list = {'fa20-cs425-g29-01.cs.illinois.edu',
                           'fa20-cs425-g29-02.cs.illinois.edu',
                           'fa20-cs425-g29-03.cs.illinois.edu'}
        self.introducer_list = introducer_list
        # set up logging
        # TODO change the config of logging, make it simple
        logging.basicConfig(filename=id, level= logging.DEBUG)

    def leaveElection(self,node_id, time):
        self.electionList[node_id] = (False,time)
        logging.info("Node %s Leave election at %s", node_id, time)

    # default status after JOIN is status
    def join(self, node_id, heartbeat, status = "ACTIVE"):
        self.list[node_id] = (heartbeat, status)
        logging.info("Node %s JOIN at %s", node_id, heartbeat)

    # update heartbeat
    def update(self, node_id, new_heartbeat, status = "ACTIVE"):
        self.list[node_id] = (new_heartbeat, status)
        #logging.info("Node %s UPDATE at %s", node_id, heartbeat)

    # node LEFT
    def left(self, node_id, heartbeat, status = "LEFT"):
        self.list[node_id] = (heartbeat, status)
        logging.info("Node %s LEFT at %s", node_id, heartbeat)

    # node SUSPECT
    def suspect(self, node_id):
        (heartbeat, status) = self.list[node_id]
        self.list[node_id] = (heartbeat, "SUSPECT")
        logging.info("Node %s SUSPECT fail since %s", node_id, heartbeat)

    """
    conflict function with remove()
    """
    def fail(self, node_id):
        (heartbeat, status) = self.list[node_id]
        self.list[node_id][1] = "FAIL"
        logging.info("Node %s FAIL since %s", node_id, heartbeat)

    # Remove a Node from the List
    def remove(self, node_id):
        logging.info("Node %s REMOVED from LIST at %s",node_id, datetime.datetime.now())
        self.list.pop(node_id)




    """
    Plot the current MyList to terminal
    """
    def plot(self):
        curr_time = datetime.datetime.now()
        print("---------------------------------------------------------")
        print("--node ", self.id, "'s list at ", curr_time, "is follow:")
        for node_id in self.list.keys():
            (heartbeat, status) = self.list[node_id]
            print("--node ",node_id, "is ", status, " with heartbeat: ", heartbeat)
        print("---------------------------------------------------------")

    def plot_elect(self):
        curr_time = datetime.datetime.now()
        print("------------This is election List---------------------------")
        print("--node ", self.id, "'s list at ", curr_time, "is follow:")
        for node_id in self.list.electionList():
            (time, status) = self.electionList[node_id]
            print("--node ", node_id, "is ", status, " with time: ", time)
        print("---------------------------------------------------------")



if __name__ == "__main__":
    MyList = MembershipList(id = "1.log")
    old_heartbeat = datetime.datetime.now()
    MyList.join("abc", old_heartbeat)
    heartbeat = datetime.datetime.now()
    MyList.join("deadman", heartbeat, status="DEAD")
    heartbeat = datetime.datetime.now()
    MyList.plot()

#     print(MyList.list)

