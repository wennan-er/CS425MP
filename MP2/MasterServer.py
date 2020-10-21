import socketserver
import socket
import threading
# import MembershipList
from random import shuffle
from collections import defaultdict

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
class MasterHandler(socketserver.BaseRequestHandler):
    
    #e.g.: {a.txt : ['fa20-cs425-g29-01.cs.illinois.edu',
    # 'fa20-cs425-g29-02.cs.illinois.edu',
    # 'fa20-cs425-g29-03.cs.illinois.edu',
    # 'fa20-cs425-g29-04.cs.illinois.edu']}
    file_list = {}
    # two dimensional dict：e.g.:
    # {a.txt : {
    # {fa20-cs425-g29-10.cs.illinois.edu : 0},
    # {fa20-cs425-g29-09.cs.illinois.edu : 1}}}
    writing_list = defaultdict(defaultdict)
    #e.g.: {a.txt : 'fa20-cs425-g29-01.cs.illinois.edu'}
    writing_file_mapper = {}

    def __init__(self, request, client_address, server):
        self.member_list = ['fa20-cs425-g29-01.cs.illinois.edu',
                'fa20-cs425-g29-02.cs.illinois.edu',
                'fa20-cs425-g29-03.cs.illinois.edu',
                'fa20-cs425-g29-04.cs.illinois.edu',
                'fa20-cs425-g29-05.cs.illinois.edu',
                'fa20-cs425-g29-06.cs.illinois.edu',
                'fa20-cs425-g29-07.cs.illinois.edu',
                'fa20-cs425-g29-08.cs.illinois.edu',
                'fa20-cs425-g29-09.cs.illinois.edu',
                'fa20-cs425-g29-10.cs.illinois.edu']
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
                MasterHandler.writing_file_mapper[filename] = from_host
                
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
            
        elif data[:4] == "WUHU":
            # help to debug and see what is the file list on the server.
            print(MasterHandler.file_list)
            print(MasterHandler.writing_list)
            print(MasterHandler.writing_file_mapper)
        
        else:
            reply = 'GET WRONG REQUEST!'
            
        #print("reply is -{}-".format(reply))
        
        self.request.send(reply.encode())
        return

    # Determine whether a file is readable. If a file exists and no one is updating it, it is readable.
    def file_is_readable(self, filename):
        if filename in MasterHandler.file_list and filename not in MasterHandler.writing_list:
            return True
        else:
            return False

    # acquire the lock by filename, return True if 1. gained, or 2. requester is the locker user, otherwise False
    # Determine whether a file is writable. It can be updated or inserted.
    def file_is_writable(self, filename, from_host):
        if (filename in MasterHandler.file_list and filename not in MasterHandler.writing_list) or (filename in MasterHandler.writing_list and from_host == MasterHandler.writing_file_mapper[filename]) or (filename not in MasterHandler.file_list):
            return True
        else:
            return False
        
    # Determine whether a file is deletable.
    # If one file exists and not being updated, it is deletable.
    def file_is_deletable(self, filename):
        if filename in MasterHandler.file_list and filename not in MasterHandler.writing_file_mapper:
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
        shuffle(self.member_list)
        for member in self.member_list:
            if member not in MasterHandler.writing_list[filename]:
                MasterHandler.writing_list[filename][member] = 0
                #print(MasterHandler.writing_list)
                return member
        
        # No available node to write, this write operation is failed!
        MasterHandler.writing_list.pop(filename)
        MasterHandler.writing_file_mapper.pop(filename)
        return 'FAILED WRITE'

    def FIND(self, filename):
        #check read availability of this file
        reply = ""
        if self.file_is_readable(filename):
            replica_hosts = MasterHandler.file_list[filename]
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
            MasterHandler.file_list.pop(filename)
            reply = "PURGE SUCCESS"
        else:
            reply = "FILE NOT EXISTS OR NOT DELETABLE"
        return reply

    # Make this write replica success. If there are 4 success replicas already, move write it to the file list
    def CONFIRM(self, confirm_node, filename):
        if confirm_node not in MasterHandler.writing_list[filename]:
            return "MISMATCH BETWEEN CONFIRM NODE AND FILENAME"
        else:
            MasterHandler.writing_list[filename][confirm_node] = 1
            cnt = 0
            replica_list = []
            for key,value in MasterHandler.writing_list[filename].items():
                if value == 1:
                    cnt += value
                    replica_list.append(key)
            if cnt >= 4:
                # Write process OK!
                MasterHandler.file_list[filename] = replica_list
                MasterHandler.writing_list.pop(filename)
                MasterHandler.writing_file_mapper.pop(filename)
                return "WRITE 4 RELPLCAS"
            else:
                return "STILL WRITING"

    def REASSIGN(self, filename):
        pass

                

# Similar to lib example
class MasterServer(socketserver.TCPServer):

    # server_address = (localhost, port)
    def __init__(self, server_address, handler_class= MasterHandler):
        socketserver.TCPServer.__init__(self, server_address, handler_class)
        return
    
    def server_activate(self):
        self.logger.debug('server_activate')
        socketserver.TCPServer.server_activate(self)
        return

    def serve_forever(self):
        self.logger.debug('waiting for request')
        self.logger.info('Handling requests, press <Ctrl-C> to quit')
        while True:
            self.handle_request()
        return

    def handle_request(self):
        self.logger.debug('handle_request')
        return socketserver.TCPServer.handle_request(self)

    def verify_request(self, request, client_address):
        self.logger.debug('verify_request(%s, %s)', request, client_address)
        return socketserver.TCPServer.verify_request(self, request, client_address)

    def process_request(self, request, client_address):
        self.logger.debug('process_request(%s, %s)', request, client_address)
        return socketserver.TCPServer.process_request(self, request, client_address)

    def server_close(self):
        self.logger.debug('server_close')
        return socketserver.TCPServer.server_close(self)

    def finish_request(self, request, client_address):
        self.logger.debug('finish_request(%s, %s)', request, client_address)
        return socketserver.TCPServer.finish_request(self, request, client_address)

    def close_request(self, request_address):
        self.logger.debug('close_request(%s)', request_address)
        return socketserver.TCPServer.close_request(self, request_address)

if __name__ == '__main__':

    address = ('localhost', 62536)  # 让内核去分配端口
    server = MasterServer(address, MasterHandler)
    #server = socketserver.ThreadingTCPServer(('127.0.0.1', 62536), MasterHandler)
    ip, port = server.server_address  # 获取分配到的端口
    print("Server on {}:{}".format(ip, port))
    server.serve_forever()
    

