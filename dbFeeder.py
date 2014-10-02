
import cPickle
import sys
dbType = sys.argv[1]
nodes = int(sys.argv[3])
dbName = sys.argv[2]
files = ['%s/%s%s.pickle' % (dbName,dbName,i) for i in range(nodes)]

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
        for key in loaded.keys():
            db[key] = loaded[key]
        del loaded

print "All operations complete"
