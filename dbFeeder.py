
import cPickle
import sys
dbType = sys.argv[1]
nodes = int(sys.argv[3])
dbName = sys.argv[2]
files = ['%s/DistDict%s.pickle' % (dbName,i) for i in range(nodes)]

print "Connecting to database:", sys.argv[2]

if dbType == 'mongo':
    import pymongo as pm
    connection = pm.Connection()
    db = connection[dbName]
    posts = db.posts
    
    for dictFile in files:
        inFile = open(dictFile,'rb')
        print "Preparing to load file", dictFile, "from pickle"
        loaded = cPickle.load(inFile); inFile.close()
        print "Preparing to reindex file",dictFile, "for db insertion"
        reindexed = [{'key':i[0],'length':i[1]} for i in loaded.items()]
        del loaded
        print "Inserting reindexed file", dictFile, "to DB"
        posts.insert(reindexed)
        del reindexed
        print "Operation complete....\n"
        
elif dbType == 'dbm':
    import dbm
    db = dbm.open(dbName+'/'+dbName+'DB', 'c')
    
    for dictFile in files:
        inFile = open(dictFile,'rb')
        print "Preparing to load file", dictFile, "from pickle"
        loaded = cPickle.load(inFile); inFile.close()
        print "Inserting iteratively from file", dictFile, "to DB"
        for key,value in loaded.iteritems():
            db[str(key)] = str(value)
        del loaded


elif dbType == 'mmap':
    
    def writeIt(key,item):
        return '%s#%s#' % (key,item)
        
    import mmap
    found = set()
    print "Opened mmap"
    temp = open(dbName+'.mmap','w')
    temp.write("deboo2014")
    temp.close()
    with open(dbName+'.mmap', "w") as f:
        mapf = mmap.mmap(f.fileno(),0)
        for dictFile in files:
            inFile = open(dictFile,'r+b')
            print "Preparing to load file", dictFile, "from pickle"
            loaded = cPickle.load(inFile); inFile.close()
            print "File loaded...."
            count = 0
            for key,item in loaded.iteritems():
                count += 1
                if count%500 == 0:
                    print count
                    mapf.flush()
                if key not in found:
                    try:
                        mapf.write(writeIt(key,item))
                        found.add(key)
                    except:
                        print count
            print "File mmapping complete\n"
    

print "All operations complete"
