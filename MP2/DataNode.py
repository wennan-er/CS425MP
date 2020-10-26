import threading
import time
import sys
#import DataNodeServer as DataNodeServer
#import MasterServer as MasterServer
from MembershipList import MembershipList
from WorkerThread import updateMembershipList
import socket, pickle
import threading
from queue import Queue
import time
import datetime
import socketserver
import select
import threading
import time
import json as json
# import MembershipList
import logging
from random import shuffle
from collections import defaultdict
from Util import Dict2List, List2Str, Str2List, randomChoose, message

MAXSIZE = 1024

DATANODE_PORT           = 8080          # not use yet
DATANODE_SERVER_PORT    = 8089          # for DataNode Server
MASTERNODE_SERVER_PORT  = 8099          # If I'm master
PUTstart = 0
GETstart = 0

"""
    TEST MODE:
        node_id -> ('localhost', node_id)

    In real, it should be
        node_id -> (node_id, constant_server_port)

"""

def findNodeAddress(node_id):
    return ('localhost', node_id)
    # return (node_id, DATANODE_SERVER_PORT)



"""
        MasterNode operate as:
            1.  A extra process calling Master Server.
            2.  A daemon process checking FileList, send out each time

        All request are handled by MasterHandle, including the following case:

        1.  "ASSIGN [filename]"
            The first step of PUT
            Choose 4 node, add (key:filename, value: [nodes, ING/SUCC/FAIL])
            * pay attention to lock part.

        2.  "FIND [filename]"
            The first step of GET, check the FileList and 
                if can't find, return "404 NOT FOUND"
            * don't forget the lock

        3.  "PURGE [filename]"
            check the fileList, find all Storage Node and tell to "DELETE a.txt"
            * lock, lock, lock

        4.  "CONFIRM [node_id] [filename]"
            sent by the PUT NODE, mark a confirmation of a replication of a.txt
            when 4 storage confirmed, mark this file successful uploaded, and write it into FileList

        5.  "REASSIGN [node_id] [filename]"
            similar to assign case, however, make this node_id as failed, then assign a new one

"""

# The variable of self hostname, the same as DataNode.node_id
local_hostname = ""

#e.g.: {a.txt : ['fa20-cs425-g29-01.cs.illinois.edu','fa20-cs425-g29-02.cs.illinois.edu','fa20-cs425-g29-03.cs.illinois.edu','fa20-cs425-g29-04.cs.illinois.edu']}
file_list = {}
# two dimensional dict：e.g.: {a.txt : {{fa20-cs425-g29-10.cs.illinois.edu : 0}, {fa20-cs425-g29-09.cs.illinois.edu : 1}}}
writing_list = defaultdict(defaultdict)
#e.g.: {a.txt : 'fa20-cs425-g29-01.cs.illinois.edu'}
writing_file_mapper = {}

# The file list version, it will be updated aftter insertion and deletion.
file_list_version = 0

# This is a fake alive list, please replace it with proper real alive list in the following code.
# member_list = ['fa20-cs425-g29-01.cs.illinois.edu',
#         'fa20-cs425-g29-02.cs.illinois.edu',
#         'fa20-cs425-g29-03.cs.illinois.edu',
#         'fa20-cs425-g29-04.cs.illinois.edu',
#         'fa20-cs425-g29-05.cs.illinois.edu',
#         'fa20-cs425-g29-06.cs.illinois.edu']

# synchronize member_list and myList.list
member_list = []
# global variable for MembershipList
membershipList = MembershipList(id=0)
isMaster = False

# This function is defined as a global function because the MasterHandler and MasterServer will call this function both.
# It is not proper to write it in some fixed class.
def broadcast_file_list():
    # get the json string of file_list
    file_list_str = json.dumps(file_list)
    for alive_host in member_list:

        message = "UPDATE {}".format(file_list_str)
        if alive_host != local_hostname:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((alive_host, DATANODE_SERVER_PORT))
                len_sent = s.send(message.encode())
                s.close()
            except Exception as ex:
                print(type(ex))
                print(ex)
                continue
        print("{} update file list success!".format(alive_host))


def backup_node(node_to_backup):
    # e.g.: node_to_backup = 'fa20-cs425-g29-06.cs.illinois.edu'
    # 遍历所有文件，找到该node上存的文件，向其他三个node依次发出"REPLICA a.txt target_node"的消息；
    # 等待10s，socket断掉或者replica失败。成功会返回一个"CONFIRM_REPLICA a.txt"的消息;失败会返回"FAILED_REPLICA a.txt"；
    # 如果该文件其余三个node也都备份失败，那么本次replica这个文件失败，print出来。然后继续备份下面的文件
    print("Start to backup node: " + node_to_backup)

    # files_need_back_up = {file1:[node1,node2,node3]}
    # files_need_back_up = dict()
    # for key, value in file_list.items():
    #     if node_to_backup in value:
    #         peer_node = list(filter(lambda x: x != node_to_backup, value))
    #         files_need_back_up[key] = peer_node

    files_need_back_up = []
    for file, nodes in file_list.items():
        if node_to_backup in nodes:
            files_need_back_up.append(file)
    if len(files_need_back_up) == 0:
        print("nothing to back up!")
        return




    print(node_to_backup + " failed. Backup: ")
    print(files_need_back_up)

    # remove the failed/left node
    for file in files_need_back_up:
        file_list[file].remove(node_to_backup)

    # back up
    for file in files_need_back_up:
        owners = file_list[file]

        success = False

        # iterate from each owner
        for backup_sender in owners:
            #shuffle(self.MyList)

            # backup_receiver is the target to store
            backup_receiver = None
            for node in membershipList.list.keys():
                # find a new node to store this file
                if node not in owners:
                    backup_receiver = node
                    break

            if backup_receiver is None:
                print("Not enough nodes, failed to backup")
                return

            # create 'SEND filename receiver'
            msg = "SEND {} {}".format(file, backup_receiver)
            print("BACKUP file {}, new_owner {}".format(file, backup_receiver))
            print(msg)

            # try:
            #     # a client to peer node
            #     peer_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #     # sent to sender
            #     peer_client.connect((backup_sender, DATANODE_SERVER_PORT))
            #     peer_client.send(msg.encode())
            #
            #     # timeout as 10
            #     peer_client.setblocking(0)
            #     ready = select.select([peer_client], [], [], 10)
            #
            #     if ready[0]:
            #         # receive response
            #         response = peer_client.recv(MAXSIZE).decode()
            #         print("in backup, master receive from sender node:" + response)
            #         response = response.split(' ')
            #         if response[0] == "BACKUP":
            #             # add this newowner
            #             print("add {} into owner of {}".format(backup_receiver, file))
            #             file_list[file].append(backup_sender)
            #
            #             # inc and broadrcast file list
            #             global file_list_version
            #             file_list_version = file_list_version + 1
            #             broadcast_file_list()
            #
            #             print("Replica file {} of failed node {} success!".format(file , node_to_backup))
            #             peer_client.close()
            #             return
            #
            #         else:
            #             # This replica operation failed, just go to the next replica datanode.
            #             peer_client.close()
            #             continue
            #     else:
            #         peer_client.close()
            # except Exception as ex:
            #     print(type(ex))
            #     print(ex)
            # a client to peer node
            peer_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # sent to sender
            peer_client.connect((backup_sender, DATANODE_SERVER_PORT))
            peer_client.send(msg.encode())

            # timeout as 10
            peer_client.setblocking(0)
            ready = select.select([peer_client], [], [], 10)

            if ready[0]:
                # receive response
                response = peer_client.recv(MAXSIZE).decode()
                print("in backup, master receive from sender node:" + response)
                response = response.split(' ')
                if response[0] == "BACKUP":
                    # add this newowner
                    print("add {} into owner of {}".format(backup_receiver, file))
                    file_list[file].append(backup_sender)

                    # inc and broadrcast file list
                    global file_list_version
                    file_list_version = file_list_version + 1
                    broadcast_file_list()

                    print("Replica file {} of failed node {} success!".format(file, node_to_backup))
                    peer_client.close()
                    return

                else:
                    # This replica operation failed, just go to the next replica datanode.
                    peer_client.close()
                    continue

    print("Replica file {} of failed node {} failed!".format(file, node_to_backup))


"""
备注：
在datanode receive时，假设receive的string存在了msg变量中，获取file_list的操作：
splits = msg.split('\n')
new_file_list = json.loads(splits[1])
这里,new_file_list就是一个新的file list了，类型是dict
"""

class MasterHandler(socketserver.BaseRequestHandler):

    def __init__(self, request, client_address, server):
        self.logger = logging.getLogger('MasterHandler')
        #self.logger.debug('__init__')
        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)
        return
        
    # TODO: start from here
    def handle(self):
        data = self.request.recv(MAXSIZE).decode()
        
        print("Receive data:{}".format(data))

        # praser
        # example: "ASSIGN a.txt"
        reply = ""
        # from_host is the hostname of the client.
        from_host = socket.gethostbyaddr(self.client_address[0])[0]
        
        splits = data.split(' ')
        if isMaster:
            if splits[0] == "ASSIGN":
                # return a node hostname if it's ok. e.g: "fa20-cs425-g29-08.cs.illinois.edu"
                filename = splits[1]
                # check write availability for this file
                if not self.file_is_writable(filename, from_host):
                    # tell the requesting node, this file is using right now.
                    reply = "FILE IN USING"
                else:
                    # assign a new node to this request if there is a node available.
                    reply = self.ASSIGN(filename)
                    writing_file_mapper[filename] = from_host

            elif splits[0] == "FIND":
                # return 4 replica hostnames, which are seperated by comma.e.g:"fa20-cs425-g29-07.cs.illinois.edu,fa20-cs425-g29-10.cs.illinois.edu,fa20-cs425-g29-03.cs.illinois.edu,fa20-cs425-g29-09.cs.illinois.edu"
                filename = splits[1]
                reply = self.FIND(filename)

            elif splits[0] == "PURGE":
                # reply "PURGE SUCCESS" if delete is OK.
                filename = splits[1]
                reply = self.PURGE(filename)

            elif splits[0] == "CONFIRM":
                confirm_node_name = splits[1]
                filename = splits[2]
                reply = self.CONFIRM(confirm_node_name, filename)
            elif splits[0] == "SLEEP":
                time.sleep(7)
                reply = "sleep 7 seconds!"

            elif data[:4] == "WUHU":
                # help to debug and see what is the file list on the server.
                print(file_list)
                print(writing_list)
                print(writing_file_mapper)
                print('File list version is{}'.format(file_list_version))
        
        else:
            reply = 'I AM NOT THE MASTER!'
            
        #print("reply is -{}-".format(reply))
        
        self.request.send(reply.encode())
        return

    # Determine whether a file is readable. If a file exists and no one is updating it, it is readable.
    def file_is_readable(self, filename):
        if filename in file_list and filename not in writing_list:
            return True
        else:
            return False

    # acquire the lock by filename, return True if 1. gained, or 2. requester is the locker user, otherwise False
    # Determine whether a file is writable. It can be updated or inserted.
    def file_is_writable(self, filename, from_host):
        if (filename in file_list and filename not in writing_list) or (filename in writing_list and from_host == writing_file_mapper[filename]) or (filename not in file_list):
            return True
        else:
            return False
        
    # Determine whether a file is deletable.
    # If one file exists and not being updated, it is deletable.
    def file_is_deletable(self, filename):
        if filename in file_list and filename not in writing_file_mapper:
            return True
        else:
            return False
        

    # should already acquire the lock
    def unlock_file_lock(self, filename):
        pass

    # assign a new node to this assign request. e.g."fa20-cs425-g29-08.cs.illinois.edu".
    # If there is no available node, return a error message!
    # 0 - is writing. 1 - success, 2 - failed
    def ASSIGN(self, filename):
        shuffle(member_list)
        for member in member_list:
            if member not in writing_list[filename]:
                writing_list[filename][member] = 0
                # TODO:starttime
                global PUTstart
                PUTstart = datetime.datetime.now()
                print("The start time of assigning",datetime.datetime.now())
                return member
        
        # No available node to write, this write operation is failed!
        writing_list.pop(filename)
        writing_file_mapper.pop(filename)


        print("After assign, the list is:")
        print(writing_list[filename])


        return 'FAILED WRITE'

    def FIND(self, filename):
        #check read availability of this file
        reply = ""
        if self.file_is_readable(filename):
            replica_hosts = file_list[filename]
            # hostnames are seperated by comma.
            reply = ",".join(replica_hosts)
        else:
            reply = "FILE NOT FOUND OR NOT READABLE"
        return reply

    def PURGE(self, filename):
        #check if a file is deletable.
        reply = ""
        if self.file_is_deletable(filename):
            # Todo: Send some delete instruction to datanode. Or some other ways?
            file_list.pop(filename)
            global file_list_version
            file_list_version = file_list_version + 1
            broadcast_file_list()
            reply = "PURGE SUCCESS"
        else:
            reply = "FILE NOT EXISTS OR NOT DELETABLE"
        return reply
                
    # Make this write replica success. If there are 4 success replicas already, move write it to the file list
    def CONFIRM(self, confirm_node, filename):

        print("confirm_node:"+confirm_node)

        if confirm_node not in writing_list[filename]:
            print("expect:")
            print(writing_list[filename])

            return "MISMATCH BETWEEN CONFIRM NODE AND FILENAME"
        else:
            writing_list[filename][confirm_node] = 1
            cnt = 0
            replica_list = []
            for key,value in writing_list[filename].items():
                if value == 1:
                    cnt += value
                    replica_list.append(key)
            if cnt >= 4:
                # TODO: endtime
                print("--------------------------------------------------")
                print("time last is",datetime.datetime.now()-PUTstart)
                print("--------------------------------------------------")
                print("The end time of assigning",datetime.datetime.now())
                # Write process OK!
                file_list[filename] = replica_list
                global file_list_version
                file_list_version = file_list_version + 1
                broadcast_file_list()
                writing_list.pop(filename)
                writing_file_mapper.pop(filename)
                return "WRITE 4 RELPLCAS"
            else:
                return "STILL WRITING"

    def REASSIGN(self, filename):
        pass

# Similar to lib example
class MasterServer(socketserver.TCPServer):

    # server_address = (localhost, port)
    def __init__(self, server_address, handler_class= MasterHandler):
        self.logger = logging.getLogger('MasterServer')
        #self.logger.debug('__init__')
        socketserver.TCPServer.__init__(self, server_address, handler_class)
        return
    
    def server_activate(self):
        #self.logger.debug('server_activate')
        socketserver.TCPServer.server_activate(self)
        return

    def serve_forever(self):
        #self.logger.debug('waiting for request')
        self.logger.info('Handling requests, press <Ctrl-C> to quit')
        while True:
            self.handle_request()
        return

    def handle_request(self):
        #self.logger.debug('handle_request')
        return socketserver.TCPServer.handle_request(self)

    def verify_request(self, request, client_address):
        #self.logger.debug('verify_request(%s, %s)', request, client_address)
        return socketserver.TCPServer.verify_request(self, request, client_address)

    def process_request(self, request, client_address):
        #self.logger.debug('process_request(%s, %s)', request, client_address)
        return socketserver.TCPServer.process_request(self, request, client_address)

    def server_close(self):
        #self.logger.debug('server_close')
        return socketserver.TCPServer.server_close(self)

    def finish_request(self, request, client_address):
        #self.logger.debug('finish_request(%s, %s)', request, client_address)
        return socketserver.TCPServer.finish_request(self, request, client_address)

    def close_request(self, request_address):
        #self.logger.debug('close_request(%s)', request_address)
        return socketserver.TCPServer.close_request(self, request_address)

    # def backup_node(self, node_to_backup):
    #     # e.g.: node_to_backup = 'fa20-cs425-g29-06.cs.illinois.edu'
    #     # 遍历所有文件，找到该node上存的文件，向其他三个node依次发出"REPLICA a.txt target_node"的消息；
    #     # 等待10s，socket断掉或者replica失败。成功会返回一个"CONFIRM_REPLICA a.txt"的消息;失败会返回"FAILED_REPLICA a.txt"；
    #     # 如果该文件其余三个node也都备份失败，那么本次replica这个文件失败，print出来。然后继续备份下面的文件
    #     print("Start to backup node: " + node_to_backup)
    #     for key,value in file_list.items():
    #         if node_to_backup in value:
    #             #delete it from file table first
    #             value.remove(node_to_backup)
    #
    #             # This key should be backup
    #             success = False
    #             for host in value:
    #                 if host != node_to_backup and host in member_list:
    #                     # this host has a replica and it is alive
    #                     message = "SEND {} {}".format(key, host)
    #                     try:
    #                         s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #                         s.connect((host, DATANODE_SERVER_PORT))
    #                         len_sent = s.send(message)
    #
    #                         # Set timeout as 10
    #                         s.setblocking(0)
    #                         ready = select.select([s], [], [], 10)
    #                         if ready[0]:
    #                             # receive response
    #                             response = s.recv(1024)
    #                             splits = response.split(' ')
    #                             if splits[0] == "BACKUP":
    #                                 success = True
    #                                 value.append(host)
    #                                 global file_list_version
    #                                 file_list_version = file_list_version + 1
    #                                 broadcast_file_list()
    #                                 print("Replica file {} of failed node {} success!".format(key, node_to_backup))
    #                                 s.close()
    #                                 break
    #                             else:
    #                                 # This replica operation failed, just go to the next replica datanode.
    #                                 s.close()
    #                                 continue
    #                         s.close()
    #                     except Exception as ex:
    #                         print(type(ex))
    #                         print(ex)
    #             if not success:
    #                 print("Replica file {} of failed node {} failed!".format(key, node_to_backup))
    #     return

#------- Below is DataNode


class DataNodeServerHandler(socketserver.BaseRequestHandler):


    def __init__(self, request, client_address, server):
        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)
        return


    # TODO Check encode/decode case
    def handle(self):
        data = self.request.recv(MAXSIZE).decode()

        print("Receive data:{}".format(data))

        # praser
        # example: "ASSIGN a.txt"
        reply = ""
        # from_host is the hostname of the client.
        from_host = socket.gethostbyaddr(self.client_address[0])[0]

        splits = data.split(' ')

        # "GET a.txt"
        if splits[0] == "GET":
            self.GET(splits[1])

        # "PUT a.txt"
        elif splits[0] == "PUT":
            msg = "ACK"
            self.request.send(msg.encode())
            self.PUT(splits[1])


        # "SEND a.txt node_id"
        elif splits[0] == "SEND":
            self.SEND(splits[1], splits[2])

        # "UPDATE "
        elif splits[0] == "UPDATE":
            print("recv an UPDATE from master")
            #print("content is :" splits[1])

            file_list_json_str = ''.join(splits[1:])
            self.UPDATE(file_list_json_str)
        pass




    # server handle "GFT sdfsfilename"
    def GET(self, sdfsfilename):

        # "r" - read mode
        sdfsfile = open(sdfsfilename, "rb")

        # read form sdfsfile, write into socket
        content = sdfsfile.read(MAXSIZE)
        while content:
            self.request.send(content)
            content = sdfsfile.read(MAXSIZE)

        sdfsfile.close()
        return



        # server handle "PUT sdfsfilename"
    def PUT(self, sdfsfilename):

        print("server start a put job, sdfsilename:" + sdfsfilename)

        # "w" - overwrite
        sdfsfile = open(sdfsfilename, "wb")


        print("open ")

        # read from socket, write into sdfs
        content = self.request.recv(MAXSIZE)
        while content:
            #print("Receive data:{}".format(content.decode()))

            sdfsfile.write(content)
            content = self.request.recv(MAXSIZE)

        sdfsfile.close()

        print("put job: " +sdfsfilename + "finished")

        return








        # MasterNode client send "SEND sdfsfilename node_id"
        # DateNode will send sdfsfile to Target DataNode
        #  - if successful, reply "BACKUP sdfsfilename node_id"
        #  - if Target DataNode failed, reply "FAIL_BACKUP sdfsfilename node_id"
        #       to master node  and finish this request
        # if this node failed, let MasterNode decide the next action
    def SEND(self, sdfsfilename, target_DataNode):

        #peer_node_address = findNodeAddress(target_DataNode)
        peer_node_address = (target_DataNode, DATANODE_SERVER_PORT)
        sdfsfile = open(sdfsfilename, "rb")

        # a client communicate with peer node to transfer
        # try:
        #     # ++++++ a peer client to target_Datanode   ++++++
        #     peer_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #     peer_client.connect(peer_node_address)
        #
        #     # send "PUT a.txt" to peer_client
        #     msg = "PUT " + sdfsfilename
        #     peer_client.send(msg.encode())
        #
        #     # wait for peer node response
        #     response = peer_client.recv(MAXSIZE).decode()
        #     print("response is ",response)
        #     if response == "ACK":
        #         pass
        #     else:
        #         print("peer node not ACK")
        #
        #     content = sdfsfile.read(MAXSIZE)
        #     while content:
        #         peer_client.send(content)
        #         content = sdfsfile.read(MAXSIZE)
        #
        #     sdfsfile.close()
        #     peer_client.close()
        #
        #     print("a thread to " + peer_node_address + "finished SENDING JOB")
        #     # ++++++        already back up             ++++++
        #
        #     # send a confirm information to master
        #     msg = "BACKUP " + sdfsfilename + target_DataNode
        #     self.request.send(msg.encode())

        # except:
        #     # backup failed, send an information to master
        #     msg = "FAIL_ACKUP " + sdfsfilename + target_DataNode
        #     self.request.send(msg.encode())
        #
        # ++++++ a peer client to target_Datanode   ++++++
        peer_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_client.connect(peer_node_address)

        # send "PUT a.txt" to peer_client
        msg = "PUT " + sdfsfilename
        peer_client.send(msg.encode())

        # wait for peer node response
        response = peer_client.recv(MAXSIZE).decode()
        print("response is ", response)
        if response == "ACK":
            pass
        else:
            print("peer node not ACK")

        content = sdfsfile.read(MAXSIZE)
        while content:
            peer_client.send(content)
            content = sdfsfile.read(MAXSIZE)

        sdfsfile.close()
        peer_client.close()

        # print("a thread to " + peer_node_address + "finished SENDING JOB")
        # ++++++        already back up             ++++++

        # send a confirm information to master
        msg = "BACKUP " + sdfsfilename + target_DataNode
        self.request.send(msg.encode())

    def UPDATE(self, file_list_json_str):
        # try:
        #     print("updating file_list")
        #     global file_list
        #     file_list = json.loads(file_list_json_str)
        #     print(file_list)
        # except Exception as ex:
        #     print(ex)
        #     print("Update my file list failed!")
        print("updating file_list")
        print(file_list_json_str)
        global file_list
        file_list = json.loads(file_list_json_str)
        print(file_list)
        print("finish updating need reverse")

# Similar to lib example
class DataNodeServer(socketserver.TCPServer):

    # server_address = (localhost, port)
    def __init__(self, server_address, handler_class=DataNodeServerHandler):

        print("this server ip is: " + str(server_address[0]))
        print("this server port is: " + str(server_address[1]))
        socketserver.TCPServer.__init__(self, server_address, handler_class)
        return

    def server_activate(self):
        #self.logger.debug('server_activate')
        socketserver.TCPServer.server_activate(self)
        return

    def serve_forever(self):
        #self.logger.debug('waiting for request')

        print("serve is running")
        #self.logger.info('Handling requests, press <Ctrl-C> to quit')
        while True:
            self.handle_request()
        return

    def handle_request(self):
        #self.logger.debug('handle_request')
        return socketserver.TCPServer.handle_request(self)

    def verify_request(self, request, client_address):
        #self.logger.debug('verify_request(%s, %s)', request, client_address)
        return socketserver.TCPServer.verify_request(self, request, client_address)

    def process_request(self, request, client_address):
        #self.logger.debug('process_request(%s, %s)', request, client_address)
        return socketserver.TCPServer.process_request(self, request, client_address)

    def server_close(self):
        #self.logger.debug('server_close')
        return socketserver.TCPServer.server_close(self)

    def finish_request(self, request, client_address):
        #self.logger.debug('finish_request(%s, %s)', request, client_address)
        return socketserver.TCPServer.finish_request(self, request, client_address)

    def close_request(self, request_address):
        #self.logger.debug('close_request(%s)', request_address)
        return socketserver.TCPServer.close_request(self, request_address)

#------ Below is the Daemon process

class DateNode:
    def __init__(self, node_id):

        # identity
        self.node_id = node_id
        local_hostname = node_id
        global membershipList
        membershipList = MembershipList(id=self.node_id)
        self.MyList = membershipList
        self.port = self.MyList.dic[node_id][0]
        self.intro = self.MyList.introducer_list

        # new variable for mp2
        self.port2 = self.MyList.dic[node_id][1]
        self.in_electionProgress = False  # indicate in election progress
        self.electionSenderQueue = Queue()
        self.electionReceiverQueue = Queue()
        self.master_thread = None


        # FOR TEST ('localhost', port)
        # self.address_ip     = address[0]
        # self.address_port   = int(address[1])

        # membershipList and fileList

        # master elector related information stored in
        #self.membershipList = MemberShipList()
        #self.fileList       = FileList()

        # real master address should be choose by a elector function
        ###### FOR TEST ONLY ######
        # self.master_address = ('localhost', MASTERNODE_SERVER_PORT)
        ###### FOR TEST ONLY ######

        ###### VARIABLES FROM MP1 ######
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
        ###### START ALL THE THREADS ######
        # but Receiver and Tiktok won't work until isInGroup
        threading_sender.start()
        threading_receiver.start()
        threading_tiktok.start()
        threading_worker.start()
        threading_checker.start()
        threading_checkMaster.start()
        threading_electionSender.start()
        threading_electionReceiver.start()

        # create a DataNode Server, let it keep running
        self.datanode_server = DataNodeServer(server_address= (self.node_id, DATANODE_SERVER_PORT))

        self.thread_server = threading.Thread(target= self.Maintain_server,
                                                 daemon= True)
        self.thread_server.start()

        # create a MasterNode Server, when a new master come out, start this server
        self.master_thread = None



        self.keyboard_listener = self.Keyboard_Listener(BroadcastModeLock= None, SenderQueue= None)


    def Keyboard_Listener(self, BroadcastModeLock, SenderQueue):

        print("start keyboard listening\n")

        while True:

            # Read from the KeyBoard Input

            keyboard_cmd = input("Waiting for CMD\n")
            keyboard_cmd = keyboard_cmd.split(' ')

            # parser the keyboard_cmd:
            try:

                """
                ops for MP2
                """

                # "put localfilename sdfsfilename"
                if keyboard_cmd[0] == "put":

                    (ops, localfilename, sdfsfilename) = keyboard_cmd
                    print("running " + ops + "cmd")
                    self.PUT(localfilename= localfilename, sdfsfilename= sdfsfilename)

                # "get sdfsfilename localfilename"
                elif keyboard_cmd[0] == "get":
                    (ops, sdfsfilename, localfilename) = keyboard_cmd
                    self.GET(localfilename= localfilename, sdfsfilename=sdfsfilename)

                # "delete sdfsfilename"
                elif keyboard_cmd[0] == "delete":
                    (ops, sdfsfilename) = keyboard_cmd
                    self.DELETE(sdfsfilename= sdfsfilename)

                # "ls sdfsfilename"
                elif keyboard_cmd[0] == "ls":
                    (ops, sdfsfilename) = keyboard_cmd
                    self.LS(sdfsfilename = sdfsfilename)

                # "store"
                elif keyboard_cmd[0] == "store":
                    self.STORE()

                # "show membershipList"
                # elif keyboard_cmd[0] == "show":
                #     self.MyList.plot()

                elif keyboard_cmd[0] == "show":
                    global membershipList
                    membershipList.plot()

                elif keyboard_cmd[0] == "master":
                    print("{} is the master".format(self.myList.Master))

                elif keyboard_cmd[0] == "KILL":
                    self.stillAlive = False
                    sys.exit()

                # CMD: LEFT node_i
                elif keyboard_cmd[0] == "LEFT":
                    self.LeftAction()
                    # set isInGroup to False
                    self.isInGroup.clear()

                # CMD: JOIN
                elif keyboard_cmd[0] == "JOIN":
                    self.JoinAction()
                    # set isInGroup to True
                    self.isInGroup.set()

                elif keyboard_cmd[0] == "member":
                    print(member_list)

                else:
                    print("WRONG CMD, PLEASE RETRY")
            except:
                print("SOMETHING WRONG IN RUNNING THIS CMD")

    def Maintain_server(self):
        self.datanode_server.serve_forever()

    def set_new_master(self):
        self.master_thread = threading.Thread(target= self.create_master_and_run)
        self.master_thread.start()
        print("start master")
        global isMaster
        isMaster = True

    def kill_old_master(self):
        if self.master_thread != None:
            #self.master_thread.terminate()
            self.master_thread = None
        global isMaster
        isMaster = False

    def create_master_and_run(self):
        masternode_server = MasterServer(
            server_address=(self.node_id, MASTERNODE_SERVER_PORT)
        )
        masternode_server.serve_forever()

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
                    server_address = (nodeId, self.MyList.dic[nodeId][0])

                    SendString = List2Str(SendList)
                    # print("Sending String heartbeat: ", SendString)
                    try:
                        # Send data
                        sent = sock.sendto(SendString.encode(), server_address)
                    except:
                        print("Can't Send heartbeat")

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
                # print("just receive:", data)
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
            # print("checking")

            # judge based on curr time
            curr_time = datetime.datetime.now()

            for node_id in list(self.MyList.list.keys()):
                (heartbeat, statues) = self.MyList.list[node_id]
                pasted = curr_time - heartbeat

                # changed from FAIL to REMOVE
                if pasted > t_failed and statues in {"ACTIVE", "JOIN", "SUSPECT"}:
                    self.MyList.remove(node_id)
                    # TODO: call master backup_node function
                    if isMaster:
                        backup_node(node_id)

                # if already FAIL or SUSPECT or LEFT, do nothing
                elif pasted > t_suspect and statues in {"ACTIVE", "JOIN"}:
                    self.MyList.suspect(node_id)

            # update membershipList wrt. global memberlist
            tmp_list = []
            global member_list
            for node_id in list(self.MyList.list.keys()):
                tmp_list.append(node_id)
            member_list = tmp_list
            time.sleep(self.sleepTime)

    def checkMasterThread(self):
        # sleep for join MembershipList
        time.sleep(3)
        while self.stillAlive:
            # Only working if is in the group
            self.isInGroup.wait()
            #print("checking for master")
            time.sleep(0.5)

            # case1: the first time node enter group, ask for master
            if self.MyList.Master == "None" and not self.in_electionProgress:
                # first node enter the group, elect itself to be master
                if len(self.MyList.list) == 1:
                    # fist time start master, create masterServer
                    # TODO: start new masterServer
                    print("start first master")
                    self.MyList.Master = self.node_id
                    self.set_new_master()
                else:
                    # loop through all nodes in membershipList, ask for master information
                    for node in self.MyList.list.keys():
                        if node == self.node_id:
                            continue
                        # message: type:ask destAddr:node, destPort:dic[node][1],data:null
                        mesg = message("ask", node, self.MyList.dic[node][1], self.node_id, self.MyList.dic[self.node_id][1])
                        # print("send msg ask to:",node)
                        # put ask message into queue, senderThread will send them to dest
                        self.electionSenderQueue.put(mesg)

            # case2: when failure happens on Master, change self.Master to False
            if self.MyList.Master != "None" and self.MyList.Master not in self.MyList.list:
                self.MyList.Master = "None"
                self.in_electionProgress = True
                minMasterId = 11
                # loop through all nodes in membershipList, find the smallest one as the masternode
                for node in self.MyList.list.keys():
                    curId = int(node.split('-')[3].split('.')[0])
                    minMasterId = min(minMasterId, curId)
                    if minMasterId == 10:
                        electNodeId = 'fa20-cs425-g29-10.cs.illinois.edu'
                    else:
                        electNodeId = 'fa20-cs425-g29-0'+str(minMasterId)+'.cs.illinois.edu'
                # send election message to sender, destAddr: electNodeId
                mesg = message("election", electNodeId, self.MyList.dic[electNodeId][1],self.node_id, self.MyList.dic[self.node_id][1])
                self.electionSenderQueue.put(mesg)

            # case3:
            if self.MyList.Master != "None":
                # when first enter or a new smaller node join in, they will change to smaller master node
                minMasterId = 11
                for node in self.MyList.list.keys():
                    curId = int(node.split('-')[3].split('.')[0])
                    minMasterId = min(minMasterId, curId)
                    if minMasterId == 10:
                        electNodeId = 'fa20-cs425-g29-10.cs.illinois.edu'
                    else:
                        electNodeId = 'fa20-cs425-g29-0' + str(minMasterId) + '.cs.illinois.edu'
                if self.MyList.Master != electNodeId:
                    # if prev master is self, now change to other, kill self's master server
                    if self.MyList.Master == self.node_id:
                        self.kill_old_master()
                        #TODO: kill master
                    mesg = message("change", electNodeId, self.MyList.dic[electNodeId][1], self.node_id,
                                   self.MyList.dic[self.node_id][1])
                    self.electionSenderQueue.put(mesg)
                    self.MyList.Master = electNodeId
                #print("current master is:", self.MyList.Master)
                self.in_electionProgress = False


    def electionSenderThread(self):
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        while self.stillAlive:
            electMsg = self.electionSenderQueue.get()
            server_address = (electMsg.destAddr, electMsg.destPort)
            data = pickle.dumps(electMsg)
            # print('sending to:', electMsg.destAddr, '  port is:', electMsg.destPort)
            # print("send msg :", electMsg.msgType)
            try:
                sent = sock.sendto(data, server_address)
            except:
                print("Can't Send message")



    def electionReceiverThread(self):
        BUFFERSIZE = 1024

        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        # Bind the socket to the port
        server_address = (self.node_id, self.port2)
        # print("Receiver Working with server_address", server_address)
        sock.bind(server_address)

        masterCount = 0
        while self.stillAlive:
            # print("I'm listening")
            msg_data, Sender = sock.recvfrom(BUFFERSIZE)

            data = pickle.loads(msg_data)
            if data.msgType == "ask":
                # print("receive ask for master from",data.sourceAddr)
                if self.MyList.Master != "None":
                    replyMsg = message("reply ask", data.sourceAddr, data.sourcePort,self.node_id, self.MyList.dic[self.node_id][1], self.MyList.Master)
                    self.electionSenderQueue.put(replyMsg)

            elif data.msgType == "reply ask":
                self.MyList.Master = data.msgData

            elif data.msgType == "broadcast master":
                self.MyList.Master = data.msgData

            elif data.msgType == "change":
                if self.MyList.Master != self.node_id:
                    # TODO: start new masterServer
                    self.set_new_master()

            elif data.msgType == "election":
                masterCount += 1
                if masterCount == len(self.MyList.list)-1:
                    self.MyList.Master = self.node_id
                    # TODO: start new masterServer
                    self.set_new_master()

                    for node in self.MyList.list.keys():
                        broadMsg = message("broadcast master", node, self.MyList.dic[node][1],self.node_id, self.MyList.dic[self.node_id][1], self.MyList.Master)
                        self.electionSenderQueue.put(broadMsg)
                    masterCount = 0



    def PUT(self, localfilename, sdfsfilename):
        # A thread do the PUT work
        def threading_putwork(sdfsfilename):

            print("a thread work has begin working")

            last_failed = True
            last_node = None



            while last_failed:

                # e.g. "ASSIGN a.txt"
                msg = "ASSIGN "+sdfsfilename


                peer_node = self.request_from_master(msg)


                # can't PUT right now
                if peer_node == "FILE IN USING":
                    print(peer_node)
                    exit()
                # no node available
                elif peer_node == "FAILED WRITE":
                    print(peer_node)
                    exit()

                else:
                    # START PEER TRANSFER

                    peer_server_address = self.get_peerserver_address(peer_node)
                    print("peer_server_ip: "    + peer_server_address[0])
                    print("peer_server_port: "  + str(peer_server_address[1]))
                    try:
                        peer_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        peer_client.connect(peer_server_address)

                        # send "PUT a.txt" to peer_client
                        msg = "PUT "+sdfsfilename
                        peer_client.send(msg.encode())

                        # wait for peer node response
                        
                        print("waiting for ACK")
                        response = peer_client.recv(MAXSIZE).decode()
                        print("receive response: " + response)
                        if response == "ACK":
                            print("receive ACK")
                        else:
                            print("peer node not ACK")

                    except:
                        print("can't connect peer node")

                    localfile = open(localfilename, "rb")

                    try:

                        print("connect peer node " + peer_node + " start trans")

                        content = localfile.read(MAXSIZE)
                        while content:
                                #print("Send data:{}".format(content).encode())
                                peer_client.send(content)
                                content = localfile.read(MAXSIZE)

                        localfile.close()
                        peer_client.close()

                        print("a thread to "+ peer_node +" finished SENDING JOB")
                        last_failed = False

                        # confirm with master
                        msg = "CONFIRM "+ peer_node + " " + sdfsfilename
                        self.request_from_master(msg)

                    except:
                        print("peer node "+ peer_node +" is failed")
                        last_failed = True
                        print("now repeating")






        f = open(localfilename, "rb")
        if f is None:
            print("CAN'T FIND LOCAL FILE")
            return

        # create a pool to control the thread
        thread_pool = []
        try:
            for i in range(4):
                thread_putworker = threading.Thread(target= threading_putwork,
                                                    args=(sdfsfilename,))
                thread_putworker.start()

                thread_pool.append(thread_putworker)

        finally:
            f.close()

    def GET(self, localfilename, sdfsfilename):
        # TODO: start get time
        global GETstart
        GETstart = datetime.datetime.now()
        print("The start time of getting", datetime.datetime.now())
        try:
            # a client only connect to master
            master_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_client.connect(self.get_masterserver_address())

            # e.g. "FIND a.txt"
            msg = "FIND " + sdfsfilename
            master_client.send(msg.encode())

            response = master_client.recv(MAXSIZE).decode()
            master_client.close()
        except:
            print("can't connect to master node")

        if response == "FILE NOT FOUND OR NOT READABLE":
            print(response)
            return
        avail_node_list = response.split(',')


        for avail_node in avail_node_list:
            # last node is "", which meaning failed
            if avail_node is None:
                # arrive here means all nodes failed.
                print("all avail nodes are FAILED")
                return

            try:
                # create a client
                peer_node_address = self.get_peerserver_address(avail_node)
                peer_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_client.connect(peer_node_address)


                # peer_client
                msg = "GET "+sdfsfilename
                peer_client.send(msg.encode())

                # write to local file
                f = open(localfilename, "wb")
                content = peer_client.recv(MAXSIZE)
                while content:
                    f.write(content)
                    content = peer_client.recv(MAXSIZE)

                f.close()
                peer_client.close()

                print("already get" + sdfsfilename)
                # TODO: end get
                print("-----------------------------------------------------------")
                print("Total time last for get is:",datetime.datetime.now()-GETstart)
                print("-----------------------------------------------------------")
                print("The end time of getting", datetime.datetime.now())
                return
            except:
                print("one peer node" + avail_node + "failed")
                print("NOW loop for next")

    def DELETE(self, sdfsfilename):
        try:
            # a client only connect to master
            master_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_client.connect(self.get_masterserver_address())

            # e.g. "PURGE a.txt\n"
            msg = "PURGE " + sdfsfilename
            master_client.send(msg.encode())
            response = master_client.recv(MAXSIZE).decode()
            master_client.close()
        except:
            print("can't connect to master node")

    """
        'ls sdfsfilename'
        List all machine address where this file is currently stored
        """

    def LS(self, sdfsfilename):
        print("======================")
        store_machines = file_list[sdfsfilename]
        print("The store machines of " + sdfsfilename + " are:\n")
        for machine in store_machines:
            print(machine)
        print("======================")

    """
    'store'
    List all files currently being stored at this machine
    """

    def STORE(self):

        print("======================")
        print("The file(s) on this machine are:\n")
        for sdfsfilename in file_list.keys():
            if self.node_id in file_list[sdfsfilename]:
                print(sdfsfilename)

        print("======================")

    # a temp client talk to master
    def request_from_master(self, msg):
        # a client only connect to master
        master_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_client.connect(self.get_masterserver_address())

        print("sending to master: " + msg)
        master_client.send(msg.encode())

        response = master_client.recv(MAXSIZE).decode()
        print("receive from master: " + response)
        master_client.close()

        return response


    # in real, this should wait for the master_address if there is no available one
    def get_masterserver_address(self):
        # If no master, pass
        if self.MyList.Master == "None":
            pass
        # return ('fa20-cs425-g29-01.cs.illinois.edu', 9999)
        return (self.MyList.Master, MASTERNODE_SERVER_PORT)


    # in real, node_id is 'fa20-cs425-g29-01.cs.illinois.edu'
    # in test, node_id is the port '8080',

    def get_peerserver_address(self, peer_node_id):
        # need to return server's address
        peerserver_address = (peer_node_id, DATANODE_SERVER_PORT)
        # peerserver_address = ('localhost', int(peer_node_id) + 9)

        return peerserver_address







if __name__ == "__main__":
    node_id = "fa20-cs425-g29-" + sys.argv[1] + ".cs.illinois.edu"
    print(f"Node name is : {node_id}")

    datanode = DateNode(node_id)

