

def compareID(myID,otherID):
    my = myID.split('-')[3]
    myid = int(my.split('.')[0])
    return min(myid,otherID)


if __name__ == "__main__":
    myID = 'fa20-cs425-g29-03.cs.illinois.edu'
    otherID = 10
    res = compareID(myID,otherID)
    emptyList = []
    if not emptyList:
        print("empty")
    print(res)