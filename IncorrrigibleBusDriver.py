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
	
	
def findDist(network,viable,occupied,pts,core,out_q):
    #solitary=[ n for n,d in network.degree_iter(with_labels=True) if d==0 ]
    #network.delete_nodes_from(solitary)
    
    t1 = datetime.datetime.now()
    distances = dict(); count = 0
    toDo = len(pts)
    print "Process %s starting run with %s detinations" % (core,len(pts))
    for p in pts:
    	distances[p] = dict()
    	try:
    		print "Pulling results for resource location:", p
	    	newDistances = nx.single_source_dijkstra_path_length(network,str(p))
	    	for pop in occupied:
	    		try:
	    			distances[p][key] = newDistances[str(pop)]
	    		except:
	    			None
	        count += 1
	        print "\tFinished for resource", p
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
	entries = len(listed)
        
        block = int(ceil(len(viable.keys())/float(cores)))
        processes = []
        master = set()
	
	print "Starting execution with %s threads and %s entries" % (cores,entries)
        for i in range(cores):
	    pts = viable.keys()[block*i:block*(i+1)]
            p = Process(target = findDist, args = (network,viable,occupied,pts,i,out_q))
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

del listed; del network

pickleOut = open('/'.join(sys.argv[1].split('/')[0:-1])+'/DistDict%s.pickle' % sys.argv[3],"wb")
cPickle.dump(distDict, pickleOut)
pickleOut.close()
sleep(3)


