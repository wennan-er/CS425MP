
import datetime
import random
import math

"""
message:use for election
   1.ask master
   2.reply master
   3.broadcast master
   3.election
"""

class message:
    def __init__(self, type, address1, port1, address2, port2, data = 0):
        self.msgType = type
        self.destAddr = address1
        self.destPort = port1
        self.sourceAddr = address2
        self.sourcePort = port2
        self.msgData = data


"""
Converse Dictionary to List
"""
def Dict2List(Dict):
    List = []
    for line in Dict.items():
        line = [line[0], line[1][0], line[1][1]]
        List.append(line)
    return List

"""
Converse List to String
"""
def List2Str(List):
    res = ""
    tmp = ""
    for line in List:
        [nodeId,heartbeat,statues] = line
        tmp = nodeId + "+" + str(heartbeat) +"+"+ statues
        res = res +tmp +"\n"
    return res


"""
Converse String to List
"""
def Str2List(Str):
    Str  = Str.split("\n")
    list = []
    for line in Str:
        if not len(line):
            break
        line = line.split("+")
        datatime_string = line[1]
        datetime_obj = datetime.datetime.strptime(datatime_string,
                                                  '%Y-%m-%d %H:%M:%S.%f')
        # change back to obj
        line[1] = datetime_obj
        list.append(line)
    return list


"""
randomly choose 2 different nodeId from candidateList
"""

def randomChoose(candidateList):
    canLen = len(candidateList)
    rand1 = math.floor(random.random()*canLen)
    rand2 = math.floor(random.random()*canLen)
    # make sure two nodeId index is different
    while rand2 == rand1:
        rand2 = math.floor(random.random() * canLen)
    chosenList = [candidateList[rand1], candidateList[rand2]]
    return chosenList



