import sys, os
import mmap
import cPickle


def refIt(text):
    return ','.join(text.split(',')[:2])
    
def writeIt(key,item):
    return '%s#%s#' % (key,item)



fileIn = sys.argv[1]
numBlocks = int(sys.argv[2])
pickleName = sys.argv[3]
pickleCells = int(sys.argv[4])
expName = fileIn.split('/')[0]

print "Counting lines for file", fileIn
inFile = open(fileIn)
numLines = sum(1 for line in inFile)
inFile.close()


indexPts = []

blockSize = numLines/numBlocks
refs = dict(); found = set()

print numLines, "found, using", numBlocks, "of size", blockSize
 
count = 0 
inFile = open(fileIn)
for i, line in enumerate(inFile):
    read = refIt(line)
    found.add(read)
    if i%blockSize == 0:
        print "Completed block", count
        found.remove(read)
        indexPts.append(read)
        if count != 0:
            refs = dict(refs.items() + {item:read for item in found}.items())
            found = set()         
    count += 1
    
refs = dict(refs.items() + {item:read for item in found}.items()) 

print indexPts
print refs

mmapKeys = []
mmaps = dict()

#for key in mmapKeys:
#    mmaps[key] = open(expName+key+'.map', 'w')

for key in mmapKeys:
    mmaps[key] = open(expName++key+'.map', 'r+')
    
pickleFiles = files = ['%s/DistDict%s.pickle' % (expName,i) for i in range(pickleCells)]

for dictFile in pickleFiles:
    inFile = open(dictFile,'rb')
    print "Preparing to load file", dictFile, "from pickle"
    loaded = cPickle.load(inFile); inFile.close()
    for key, item in loaded.iteritems():
        mmaps[refs(refIt(key))].write(writeIt(key,item))
        
print "It finished, surprisingly"
        
    