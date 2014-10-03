import sys, os
import mmap
import cPickle


def refIt(text):
    return ','.join(text.split(',')[:2])
    
def writeIt(key,item):
    return '%s#%s#' % (key,item)



fileIn = sys.argv[1]
numBlocks = sys.argv[2]
pickleName = sys.argv[3]
pickleCells = sys.argv[4]
expName = file.split('/')[0]

numLines = sum(1 for line in open(fileIn))

indexPts = []

blockSize = numLines/numBlocks
refs = dict(); found = set()


inFile = open(fileIn)
for i, line in enumerate(fileIn):
    read = refIt(line)
    found.add(read)
    if i%blockSize == 0:
        found.remove(read)
        indexPts.append(read)
        if i != 0:
            refs = dict(refs.items() + {item:read for item in found}.items())
            found = set()         
    i += 1
    
refs = dict(refs.items() + {item:read for item in found}.items()) 

print indexPts
print refs

mmapKeys = []
mmaps = dict()

#for key in mmapKeys:
#    mmaps[key] = open(expName+key+'.map', 'w')

for key in mmapKeys:
    mmaps[key] = open(expName++key+'.map', 'r+')
    
pickleFiles = files = ['%s/DistDict%s.pickle' % (exName,i) for i in range(pickleCells)]

for dictFile in pickleFiles:
    inFile = open(dictFile,'rb')
    print "Preparing to load file", dictFile, "from pickle"
    loaded = cPickle.load(inFile); inFile.close()
    for key, item in loaded.iteritems():
        mmaps[refs(refIt(key))].write(writeIt(key,item))
        
print "It finished, surprisingly"
        
    