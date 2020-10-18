import InterNode_Comm
import threading
import time









class DateNode:
    def __init__(self, node_id, address):

        # id and address
        self.node_id = node_id
        self.address = address

        # membershipList and fileList

        # master elector related information stored in
        self.membershipList = MemberShipList(
        self.fileList       = FileList()

        # TODO: Heartbeats Set up
        Heartbeat_process = HeartbeatSetUp()


        # TODO: Lock and critical information
        # master, lo






        self.client = MyClient()
        self.client.getNewList()

    """
    Call MP1 stuff, life-support Heartbeating System
    """
    def HeartbeatSetUp(self):
        pass


    """
    Create a client connect to server, 
    return a targetSocket
    """
    def Createclient(self, targetNode):

        # read targetNode's (address, port) for the list

        # set up

        # connect

        targetSocket = None
        return targetSocket


    """
    Each sending node divide a sending job into 4 tasks, handled by 4 threads
    """
    def PUT(self, filename):
        # check file size


        thread_pool = []
        # TODO, list multithread?
        storage_plan = []
        # max failed number = 3, so do it 4 times
        for i in range(4):
            thread_pool.append(threading.)
        #
        # # wait for all 4 threads success
        # if all(thread_pool):
        #     send_successful_put(self.master, filename, storage_plan)

        wait(thread)




    """
    Each thread individually 1. request a storage node from the MasterNode,
                             2. send the file to the storage node
                             3. if failed, re-request again
                             4. if successfully send, change it's result to True, and listening to if new_MasterNode changed
                             5. if new_MasterNode, let him new the result
    """
    def thread_put_work(self, thread_id, filename, tas):
        # 0. request a storage node:
        StorageNode = None
        # TODO master could only decide when know other
        while StorageNode is None or StorageNode == "FAILED":
            StorageNode = Send_empty_Request(self.master, filename)

            # 1. send the file to storage node
            send_result = client_file(StorageNode, filename)

            # fail to send, need a reassign
            if send_result == False:
                StorageNode = "FAILED"
                continue

            # 2. successful send a file
            else:
                storage_plan[thread_id] = True

    """
    Create a new client, send a PUT request to target node
    """
    def client_PUT(self, targetNode, filename):
        targetSocket = createClient(targetNode)
        InterNode_Comm.readLocal_writeSocket(targetSocket, filename)

    """
    Create a new client, send a GET request to target node
    """
    def client_GET(self, targetNode, filename):
        targetSocket = createClient(targetNode)
        InterNode_Comm.readSocket_writeLocal(targetSocket, filename)





    def GET(self):

        SendingNode = None

        while SendingNode is None or SendingNode == "FAILED":
            SendingNode   = client_GET_Request(self.master, filename)

            if SendingNode == "Not right now":
                print("Master Node says not right now, sleep 3")
                time.sleep(3)
                continue

            elif SendingNode == "404 can't find the file":


            result = ReceiveFileFrom(SendingNode, filename)

            # something wrong with sending node, need a new one
            if result == "FAILED:
                SendingNode == "FAILED"
                continue

            # success storage
            else:
                # write to log



    """
    A process consistently check the status of Master
    """
    def MasterChecking(self):



    """
    Lock-related
    """
    def get_currMaster(self):

        self.Master



    """
    """
    def STORE(self):

        print(self.Fileist)