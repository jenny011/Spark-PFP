import os, math
import json

from fpGrowth import buildAndMine
import threading

def countDB(dbdir, database, interval):
    dbSize = 0
    expDBdir = os.path.join(dbdir, "interval_{0}_{1}".format(database, interval))
    for filename in os.listdir(expDBdir):
        if filename.endswith(".txt"):
            with open(os.path.join(expDBdir, filename), 'r') as f:
                for line in f:
                    if line:
                        dbSize += 1
    return dbSize

def scanDB(fpath, delimiter=" "):
    db = []
    with open(fpath,'r') as f:
        for line in f:
            if line:
                db.append(line.strip().split(delimiter))
    return db

def groupID(index, partition):
    return index % partition

def sortByFlist(trx, Flist):
    temp = []
    for i in trx:
        if i in Flist:
            temp.append(i)
    return sorted(temp, key = lambda i: Flist[i])

def groupDependentTrx(trx, itemGidMap):
    GTrxMap = {}
    for i in range(len(trx)):
        gid = itemGidMap[trx[i]]
        GTrxMap[gid] = trx[:i+1]
    return [(k,v) for k, v in GTrxMap.items()]


def writeFMapToJSON(FMap, fpath):
    with open(fpath, 'w') as f:
        json.dump(FMap, f)

def writeFlistToJSON(Flist, fpath):
    Fdict = {}
    for kv in Flist:
        Fdict[kv[0]] = kv[1]
    with open(fpath, 'w') as f:
        json.dump(Fdict, f)

def readFlistFromJSON(fpath):
    with open(fpath, 'r') as f:
        ret = json.load(f)
    return ret
