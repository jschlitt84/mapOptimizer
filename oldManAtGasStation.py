import networkx as nx
import cPickle
import datetime
import sys, os

from multiprocessing import Process, Queue, cpu_count
from math import ceil
from time import sleep

trimRadius = 10

def inRange(pt,xMax,xMin,yMax,yMin):
    return pt[0]<=xMax and pt[0]>= xMin and pt[1]<=yMax and pt[1]>=yMin 
    

def trimNet(network,pt1,pt2,trimRadius):
    xMax = max(pt1[0],pt2[0])+trimRadius
    xMin = min(pt1[0],pt2[0])-trimRadius
    yMax = max(pt1[1],pt2[1])+trimRadius
    yMin = min(pt1[1],pt2[1])-trimRadius
    isGood = lambda x: inRange(eval(x),xMax,xMin,yMax,yMin)
    nodeList = [node for node in network.nodes() if isGood(node)]
    return network.subgraph(nodeList)

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
    toDo = len(pts)
    print "Process %s starting run with %s entries" % (core,len(pts))
    pts = [list(eval(pt.replace('\n',''))) for pt in pts]
    for p in pts:
    	subNet = trimNet(network,[p[0],p[1]],[p[2],p[3]],trimRadius)
    	if True:
	     length = nx.shortest_path_length(subNet,source=str([p[0],p[1]]),target=str([p[2],p[3]]),weight='weight')
	else:
	#except Exception,e: 
	     if count%50 == 0:
	         print str(e)
	     length = -1
	distances[str(rank(p[0],p[1],p[2],p[3]))] = int(length)
	count += 1
	if count%500 == 0:
		print 'Core: %s   Count: %s   Length: %s  Percent: %s' % (core,count,length, count/float(toDo))
    print "Process %s Distance tabulation complete!" % core
    out_q.put(distances) 
    

def getNet(listed,network,penalty):
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
        for key,item in merged.iteritems():
        	if item == -1:
        		merged[key] = penalty
	return merged

fileIn = open(sys.argv[1])
listed = list(set(fileIn.readlines()))
fileIn.close()

pickleIn = open(sys.argv[3],'rb')	
struct = cPickle.load(pickleIn)
pickleIn.close()

network = struct['network']

distDict = getNet(listed,network,sys.argv[4])
print distDict
#for key,item in distDict.iteritems():
#	print key,item

del listed; del network

pickleOut = open('/'.join(sys.argv[1].split('/')[0:-1])+'/DistDict%s.pickle' % sys.argv[2],"wb")
cPickle.dump(distDict, pickleOut)
pickleOut.close()
sleep(3)


