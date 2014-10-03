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

numLines = 0
print "Counting occurances per key"
keyCts = dict()
inFile = open(fileIn)
for i, line in enumerate(inFile):
    numLines += 1
    read = refIt(line)
    try:
        keyCts[refIt(line)] += 1
    except:
        keyCts[refIt(line)] = 1
        
print keyCts
quit()

blockSize = numLines/numBlocks
refList = dict(); index = 0; x = 0

for key, value in keyCts.iteritems():
    x += value
    refList[key] = index
    if x > blockSize:
        x = 0
        index += 1
    

refs = dict(); found = set()

print numLines, "found, using", numBlocks, "of size", blockSize
print refList

outFile = open(expName+'Refs.pickle','w')
cPickle.dump(refList,outFile)
quit()

mmapKeys = []
mmaps = dict()

#for key in mmapKeys:
#    mmaps[key] = open(expName+key+'.map', 'w')

for key in range(index):
    mmaps[key] = open(expName++key+'.map', 'r+')
    
pickleFiles = files = ['%s/DistDict%s.pickle' % (expName,i) for i in range(pickleCells)]

for dictFile in pickleFiles:
    inFile = open(dictFile,'rb')
    print "Preparing to load file", dictFile, "from pickle"
    loaded = cPickle.load(inFile); inFile.close()
    for key, item in loaded.iteritems():
        mmaps[refs(refIt(key))].write(writeIt(key,item))
        
print "It finished, surprisingly"
        
    