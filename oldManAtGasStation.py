import networkx as nx
import cPickle
import datetime
import sys, os

from multiprocessing import Process, Queue, cpu_count
from math import ceil
from time import sleep


def rank(x1,y1,x2,y2):
    if x1 < x2:
        return x1,y1,x2,y2
    elif x1 > x2:
        return x2,y2,x1,y1
    elif y1 < y2:
        return x1,y1,x2,y2
    else:
        return x2,y2,x1,y1

def findDist(network,pts,core,out_q):
    t1 = datetime.datetime.now()
    distances = dict(); count = 0
    print "Process %s starting run with %s entries" % (core,len(pts))
    pts = [list(eval(pt.replace('\n',''))) for pt in pts]
    for p in pts:
	length = nx.shortest_path_length(network,source=str([p[0],p[1]]),target=str([p[2],p[3]]),weight='weight')

	distances[str(rank(p[0],p[1],p[2],p[3]))] = length
	count += 1
	if count%1 == 0:
		print 'Core: %s   Count: %s   Length: %s' % (core,count,length)
    print "Process %s Distance tabulation complete!" % core
    out_q.put(distances) 
    

def getStats(listed,network):
	"""Return distance stats"""
	cores = cpu_count()
        out_q = Queue()
	entries = len(listed)
        
        block = int(ceil(len(listed)/float(cores)))
        processes = []
	
	print "Starting execution with %s threads and %s entries" % (cores,entries)
        for i in range(cores):
	    pts = listed[block*i:block*(i+1)]
            p = Process(target = findDist, args = (network,pts,i,out_q))
            processes.append(p)
            p.start()
            merged = {}
        for i in range(cores):
            merged.update(out_q.get())
        for p in processes:
            p.join()
	return merged

fileIn = open(sys.argv[1])
listed = fileIn.readlines()
fileIn.close()

pickleIn = open(sys.argv[3],'rb')	
struct = cPickle.load(pickleIn)
pickleIn.close()

network = struct['network']

distDict = getStats(listed[0:30],network)
print distDict
#for key,item in distDict.iteritems():
#	print key,item

del listed; del network

pickleOut = open('DistDict%s.pickle' % sys.argv[2],"wb")
cPickle.dump(distDict, pickleOut)
pickleOut.close()
sleep(3)


