import socketserver
import socket
import select
import threading
import time
# import MembershipList
import logging
from random import shuffle
from collections import defaultdict
import json as json

MAXSIZE = 1024


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

logging.basicConfig(level=logging.DEBUG,format='%(name)s: %(message)s',)

#e.g.: {a.txt : ['fa20-cs425-g29-01.cs.illinois.edu','fa20-cs425-g29-02.cs.illinois.edu','fa20-cs425-g29-03.cs.illinois.edu','fa20-cs425-g29-04.cs.illinois.edu']}
file_list = {}
# two dimensional dict：e.g.: {a.txt : {{fa20-cs425-g29-10.cs.illinois.edu : 0}, {fa20-cs425-g29-09.cs.illinois.edu : 1}}}
writing_list = defaultdict(defaultdict)
#e.g.: {a.txt : 'fa20-cs425-g29-01.cs.illinois.edu'}
writing_file_mapper = {}

# Change it as you like.
datanode_server_port = 6789
master_server_port = 62536

# The file list version, it will be updated aftter insertion and deletion.
file_list_version = 0

# This is a fake alive list, please replace it with proper real alive list in the following code.
member_list = ['fa20-cs425-g29-01.cs.illinois.edu',
        'fa20-cs425-g29-02.cs.illinois.edu',
        'fa20-cs425-g29-03.cs.illinois.edu',
        'fa20-cs425-g29-04.cs.illinois.edu',
        'fa20-cs425-g29-05.cs.illinois.edu',
        'fa20-cs425-g29-06.cs.illinois.edu']


# This function is defined as a global function because the MasterHandler and MasterServer will call this function both.
# It is not proper to write it in some fixed class.
def UPDATE():
    # get the json string of file_list
    file_list_str = json.dump(file_list)
    for alive_host in member_list:
        # TODO: 这里需要改一下，判断这个alive的不是自己
        message = "UPDATE\n{}".format(file_list_str)
        if alive_host != "自己":
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((alive_host, datanode_server_port))
                len_sent = s.send(message)
                s.close()
            except Exception as ex:
                print(type(ex))
                print(ex)
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
            reply = 'GET WRONG REQUEST!'
            
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
                #print(writing_list)
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
            #UPDATE()
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
                # Write process OK!
                file_list[filename] = replica_list
                global file_list_version
                file_list_version = file_list_version + 1
                #UPDATE()
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
    
    def backup_node(self, node_to_backup):
        # e.g.: node_to_backup = 'fa20-cs425-g29-06.cs.illinois.edu'
        # 遍历所有文件，找到该node上存的文件，向其他三个node依次发出"REPLICA a.txt target_node"的消息；
        # 等待10s，socket断掉或者replica失败。成功会返回一个"CONFIRM_REPLICA a.txt"的消息;失败会返回"FAILED_REPLICA a.txt"；
        # 如果该文件其余三个node也都备份失败，那么本次replica这个文件失败，print出来。然后继续备份下面的文件
        print("Start to backup node: " + node_to_backup)
        for key,value in file_list.items():
            if node_to_backup in value:
                #delete it from file table first
                value.remove(node_to_backup)
                
                # This key should be backup
                success = False
                for host in value:
                    if host != node_to_backup and host in member_list:
                        # this host has a replica and it is alive
                        message = "SEND {} {}".format(key, host)
                        try:
                            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            s.connect((host, datanode_server_port))
                            len_sent = s.send(message)
                            
                            # Set timeout as 10
                            s.setblocking(0)
                            ready = select.select([s], [], [], 10)
                            if ready[0]:
                                # receive response
                                response = s.recv(1024)
                                splits = response.split(' ')
                                if splits[0] == "BACKUP":
                                    success = True
                                    value.append(host)
                                    global file_list_version
                                    file_list_version = file_list_version + 1
                                    #UPDATE()
                                    print("Replica file {} of failed node {} success!".format(key, node_to_backup))
                                    s.close()
                                    break
                                else:
                                    # This replica operation failed, just go to the next replica datanode.
                                    s.close()
                                    continue
                            s.close()
                        except Exception as ex:
                            print(type(ex))
                            print(ex)
                if not success:
                    print("Replica file {} of failed node {} failed!".format(key, node_to_backup))
        return
        

if __name__ == '__main__':

    address = ('localhost', master_server_port)  # 让初始化时，把localhost换成自己的域名
    server = MasterServer(address, MasterHandler)
    #server = socketserver.ThreadingTCPServer(('127.0.0.1', 62536), MasterHandler)
    ip, port = server.server_address  # 获取分配到的端口
    print("Server on {}:{}".format(ip, port))
    server.serve_forever()
    

