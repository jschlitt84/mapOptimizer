import pymongo as pm
import cPickle
import sys
nodes = int(sys.argv[2])
dbName = sys.argv[1]
files = ['dbFeed/DistDict%s.pickle' % i for i in range(nodes)]

print "Connecting to database:", sys.argv[1]

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

print "All operations complete"
