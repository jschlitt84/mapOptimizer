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


def rank(x1,y1,x2,y2):
    if x1 < x2:
        return x1,y1,x2,y2
    elif x1 > x2:
        return x2,y2,x1,y1
    elif y1 < y2:
        return x1,y1,x2,y2
    else:
        return x2,y2,x1,y1
        
def getKey(key1,key2):
	pt1 = listFromStr(key1)
	pt2 = listFromStr(key2)
	return str(rank(pt1[0],pt1[1],pt2[0],pt2[1]))
	
	
def findDist(network,viable,occupied,core,out_q):
    #solitary=[ n for n,d in network.degree_iter(with_labels=True) if d==0 ]
    #network.delete_nodes_from(solitary)
    
    t1 = datetime.datetime.now()
    distances = dict(); count = 0
    toDo = len(viable)
    print "Process %s starting run with %s destinations" % (core,toDo)
    for rLoc in viable:
    	distances[str(rLoc)] = dict()
    	try:
    		print "Pulling results for resource location:", rLoc
	    	newDistances = nx.single_source_dijkstra_path_length(network,str(rLoc))
	    	for pop in occupied:
	    		try:
	    			distances[str(rLoc)][str(pop)] = newDistances[str(pop)]
	    		except:
	    			None
	        count += 1
	        print "\tFinished for resource", rLoc
		if count%10 == 0:
			print (datetime.datetime.now()-t1)
			print 'Core: %s   Count: %s   Percent: %s' % (core,count,count/float(toDo))
	except:
		print "Unconnected resource"
    print "Process %s Distance tabulation complete!" % core
    out_q.put(distances) 
    		
    

def getNet(network,viable,occupied,penalty):
	"""Return distance stats"""
	cores = cpu_count()/2
        out_q = Queue()
        
        block = int(ceil(len(viable)/float(cores)))
        processes = []
        master = set()
        entries = len(viable)
	
	print "Starting execution with %s threads and %s entries" % (cores,entries)
        for i in range(cores):
	    pts = viable[block*i:block*(i+1)]
            p = Process(target = findDist, args = (network,pts,occupied,i,out_q))
            processes.append(p)
            p.start()
            merged = {}
        for i in range(cores):
            merged.update(out_q.get())
        for p in processes:
            p.join()
	return merged

print "loading from pickle"
pickleIn = open(sys.argv[1],'rb')	
struct = cPickle.load(pickleIn)
pickleIn.close()

network = struct['network']
viable = struct['viable']
occupied = struct['occupied']

print "sending for analysis"
distDict = getNet(network,viable,occupied,sys.argv[2])

#print distDict
#for key,item in distDict.iteritems():
#	print key,item

del network

pickleOut = open('/home/NDSSL/study/mapOptimizer/DistDictNew.pickle',"wb")
cPickle.dump(distDict, pickleOut)
pickleOut.close()
sleep(3)


