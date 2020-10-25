from Util import Dict2List
import MembershipList as M
import datetime
import time





def updateMembershipList(MyList, RecList, t_session, My_node_id):
    t_ignore = datetime.timedelta(seconds=6)
    t_session = datetime.timedelta(seconds= t_session)
    # print("updating")

    for line in RecList:
        [nodeId, heartbeat, statues] = line

        """ 
        id in RecList not MyList
        """
        if nodeId not in MyList.list:
            """ 
            compare current time and receive time
            if the message is too old, greater than t_update ago, ignore
            """
            t_current = datetime.datetime.now()
            if t_current - heartbeat > t_ignore:
                continue
            if statues == "ACTIVE":
                # false join: log as JOIN, send to other nodes as ACTIVE
                MyList.join(nodeId, heartbeat, "ACTIVE")
            elif statues == "JOIN":
                # true join:  log and send as JOIN
                MyList.join(nodeId, heartbeat, "JOIN")
            elif statues == "LEFT":
                MyList.left(nodeId, heartbeat, "LEFT")

        # id in RecList and MyList
        else:
            # I don't need others to tell what I'm doing
            if nodeId == My_node_id:
                continue

            (myHeartbeat, myStatus) = MyList.list[nodeId]

            # Only update if the information is newer
            if myHeartbeat < heartbeat:
                is_updated = True

                # case1: MyList statues is ACTIVE
                if myStatus == "ACTIVE":
                    if statues in {"ACTIVE", "SUSPECT", "JOIN"}:
                        MyList.update(nodeId, heartbeat, "ACTIVE")
                    # case: LEFT
                    else:
                        MyList.left(nodeId, heartbeat, "LEFT")

                # case2: MyList statues is LEFT
                elif myStatus == "LEFT":
                    if statues == "ACTIVE":
                        MyList.join(nodeId, heartbeat, "ACTIVE")
                    elif statues == "JOIN":
                        MyList.join(nodeId, heartbeat, "JOIN")
                    elif statues == "LEFT":
                        MyList.update(nodeId, heartbeat, "LEFT")
                    # case: SUSPECT
                    else:
                        print("Wired case, should already left but heard higher heartbeats")
                        MyList.update(nodeId, heartbeat, "LEFT")

                # case3: MyList statues is SUSPECT
                elif myStatus == "SUSPECT":
                    if statues in {"ACTIVE", "SUSPECT"}:
                        MyList.update(nodeId, heartbeat, "ACTIVE")
                    elif statues == "LEFT":
                        MyList.left(nodeId, heartbeat, "LEFT")
                    # case: JOIN
                    else:
                        MyList.join(nodeId, heartbeat, "JOIN")

                # case4: MyList statues is JOIN
                else:
                    if statues in {"ACTIVE", "SUSPECT", "JOIN"}:
                        MyList.update(nodeId, heartbeat, "ACTIVE")
                    # case: LEFT
                    else:
                        MyList.update(nodeId, heartbeat, "LEFT")
    MyList.plot()
