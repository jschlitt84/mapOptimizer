import sys, os
import pickle
import subprocess 

from math import factorial, ceil
from time import sleep
from random import shuffle


def rank(x1,y1,x2,y2):
    if x1 < x2:
        return x1,y1,x2,y2
    elif x1 > x2:
        return x2,y2,x1,y1
    elif y1 < y2:
        return x1,y1,x2,y2
    else:
        return x2,y2,x1,y1


def getNumProc(name, cluster=True):
	if cluster:
		temp = [item for item in (os.popen("qstat | grep "+name).read().split('\n')) if ' C ' not in item and name in item]
		if temp != []:
			temp = len(temp)
			print "Running", temp, "items on cluster"
			return temp
		else:
			return 0
	else:
		return len(os.popen("ps aux | grep "+name).read().split('\n')) - 1


def qsubName(name, number, directory):
	return directory + name + '/' + name + str(number) + '.qsub'


def makeQsubs(name,pos,qsub,args):
	directory = os.getcwd() + '/'
	expName = name
	changed = lambda x: x.replace('$EXPDIRECTORY',directory).replace('$EXPNAME',expName).replace('$EXPPOS',str(pos)).replace('$ARGS',args).replace('\n','')
	qsub = '\n'.join([changed(line) for line in qsub]) + '\n'

	fileOut = open(qsubName(name,pos,directory),'w')
	fileOut.write(qsub)
	fileOut.close()


print "Preparing to load pickled data structure..."

try:
	netName = sys.argv[1]
	pickleIn = open(netName,'rb')
except:
	netName = 'network.pickle'
	pickleIn = open(netName,'rb')	

cores = [x for x in sys.argv if x.startswith('-c=')]
try:
	cores = int(cores[0].replace('-c=',''))
except:
	cores = 12

qsub = [x for x in sys.argv if x.startswith('-q=')]
try:
	qsub = str(qsub[0].replace('-q=',''))
except:
	qsub = 'pecos.qsub'

name = [x for x in sys.argv if x.startswith('-n=')]
try:
	name = str(name[0].replace('-n=',''))
except:
	name = 'mapOpOut'

time = [x for x in sys.argv if x.startswith('-t=')]
try:
	time = int(time[0].replace('-t=',''))
except:
	time = 24
	
penalty = [x for x in sys.argv if x.startswith('-p=')]
try:
	penalty = int(time[0].replace('-p=',''))
except:
	penalty = 24

qsubLoaded = open(qsub).readlines()


print "Cores: %s   Qsub: %s   Name: %s   Time: %s   Penalty: %s" % (cores,qsub,name,time,penalty)

struct = pickle.load(pickleIn)
pickleIn.close()


network = struct['network']
mapped = struct['map']
towns = struct['towns']


pts = []
pairs = set()
xLen = len(mapped)
yLen = len(mapped[0])

for x in range(xLen):
	for y in range(yLen):
		if mapped[x][y] != -1:
			pts.append([x,y])

numPts = len(pts)
numTowns = len(towns)

print numPts,"available spaces found"
print "Preparing to fill expected values set of approximate size",(numPts-1)*numTowns

sleep(1)

iter = 0
for i in towns:
	for j in pts:
		pairs.add(str(rank(i[0],i[1],j[0],j[1])))
		iter += 1
		if iter%250000 == 0:
			print iter

numPairs = len(pairs)
pairs = list(pairs)
shuffle(pairs)

print numPairs, "total combinations found"

print "Preparing to generate qsubs"

block = int(ceil(numPairs)/float(cores))

if not os.path.exists(name):
	os.makedirs(name)

workingDir = os.getcwd() + '/' 
for i in range(cores):
	chunk = pairs[block*i:block*(i+1)]
	listOut = open('%s/netSlice%s.txt' % (name,i),"w")
	listOut.write('\n'.join(chunk))
	listOut.close()
	makeQsubs(name,i,qsubLoaded,workingDir+netName+' '+str(penalty))

print "Waiting for available slots"

count = 31
while count >30:
	running = [item for item in (os.popen("qstat").read().split('\n')) if ' C ' not in item]
	count = len(running)
	sleep(5)

print "Beginning job submission"
for i in range(cores):
	subprocess.call(["qsub",qsubName(name,i,workingDir)])
	sleep(5)
	
count = 1
while count != 0:
	count = getNumProc(pool,cluster)
	sleep(5)

print "Experiment complete, please merge resultant dictionaries with dictMerge.py"

