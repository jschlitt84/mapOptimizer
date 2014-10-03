import networkx as nx
import cPickle
import datetime
import sys, os

from multiprocessing import Process, Queue, cpu_count
from math import ceil, sqrt
from time import sleep

trimRadius = 15
trimWidth = 15

def listFromStr(text):
    return [int(entry) for entry in list(text.replace('[','').replace(']','').replace('(','').replace(')','').split(','))]

def dist(pt1,pt2,pt3): # x3,y3 is the point
    px = pt2[0]-pt1[0]; py = pt2[1]-pt1[1]
    i = px*px + py*py
    u =  ((pt3[0] - pt1[0]) * px + (pt3[1] - pt1[1]) * py) / float(i)
    if u > 1:
        u = 1
    elif u < 0:
        u = 0
    x = pt1[0] + u * px; y = pt1[1] + u * py
    dx = x - pt3[0]; dy = y - pt3[1]
    return sqrt(dx*dx + dy*dy)


def inRange(pt,pt1,pt2,xMax,xMin,yMax,yMin):
    #return pt[0]<=xMax and pt[0]>= xMin and pt[1]<=yMax and pt[1]>=yMin

    if pt[0]<=xMax and pt[0]>= xMin and pt[1]<=yMax and pt[1]>=yMin:
        try:
        	if dist(pt1,pt2,pt) < trimWidth:
        	    return True
        except:
        	return True
    return False
        

def trimNet(network,pt1,pt2,trimRadius):
    xMax = max(pt1[0],pt2[0])+trimRadius
    xMin = min(pt1[0],pt2[0])-trimRadius
    yMax = max(pt1[1],pt2[1])+trimRadius
    yMin = min(pt1[1],pt2[1])-trimRadius
    nodeList = [node for node in network.nodes() if inRange(listFromStr(node),pt1,pt2,xMax,xMin,yMax,yMin)]
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
    distances = set(); count = 0
    toDo = len(pts)
    print "Process %s starting run with %s entries" % (core,len(pts))
    pts = set([listFromStr(pt.replace('\n','')) for pt in pts]
    for p in pts:
    	#subNet = trimNet(network,[p[0],p[1]],[p[2],p[3]],trimRadius)
    	#if True:
    	try:
	     length = nx.shortest_path_length(network,source=str([p[0],p[1]]),target=str([p[2],p[3]]),weight='weight')
	#else:
	except Exception: 
	     length = -1
	distances.add(str(rank(p[0],p[1],p[2],p[3])))+' '+str(int(length)) 
	#distances[str(rank(p[0],p[1],p[2],p[3]))] = int(length)
	count += 1
	if count%500 == 0:
		print 'Core: %s   Count: %s   Length: %s  Percent: %s' % (core,count,length, count/float(toDo))
    print "Process %s Distance tabulation complete!" % core
    out_q.put({core:distances}) 
    

def getNet(listed,network,penalty):
	"""Return distance stats"""
	cores = cpu_count()
        out_q = Queue()
	entries = len(listed)
        
        block = int(ceil(len(listed)/float(cores)))
        processes = []
        master = set()
	
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
        for core in range(cores):
        	master = master.union(merged[core])
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


