nResource = 10
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <headingcell level=1>

# Resource Allocation Distance Optimizer v2

# <headingcell level=3>

# The goal of this program is to find locations for a finite set of resources such that the minimal mean distance from resource to population member is achieved without leaving any members at an excessive distance from the nearest resource.

# <codecell>

from random import randint, sample, random, choice, uniform
from numpy import mean, std, zeros, average, array, ones, arange
from copy import deepcopy
from math import ceil, sqrt
from geopy.distance import great_circle, vincenty
from geopy import geocoders

import networkx as nx
import datetime

import cPickle

# Experiment Globals

# <codecell>

meanTilt = 0.97 #Score = tilt* mean distance + (1-tilt)* max distance
figSize = 10
big = 999999999999999

try:
    print cfg.keys()
except:
    cfg = {'travel':'network',
           'distance':'manhattan',
           'population':'density',
           'obstacles':0.2,
           'extras':dict(),
           'precision':'int',
           'penalty':300,
           'wander':8,
           'inCityD':True,
           'inCityH':True,
           'threads':1,
           'landFill':1,
           'walkingSpeed':2,
           'roadLimit':0.99,
           'permanent':False,
           'pickledRun':'network.pickle',
           'pickledDist':'network.pickle',
           'resourceFile':'WholeCountry/etus.txt',
           'populationFile':'RevisedGuess/ebolaguess2.txt',
           'roadFile':'WholeCountry/travelnet.txt',
           'exclusionFile':'WholeCountry/etus.txt',
           'useDB':'EbolaMonroviaFull',
           'filterAccessible':False,
           'trimRadius':20,
           'sparsifyX':False,
           'sparsifyY':False,
           'sparsifyLimit':0.001,
           'distMethod':'reverse',
           'netMode':'proximity',
           'netRadius':10,
           'extras':{'permanent':[],'tried':dict()}}

cfg['pickledRun'] = False
cfg['pickledDist'] = False
#cfg['resourceFile']='SELiberiaFiles/hospitals.txt',


#Travel modes = crow or path
#Population modes = density or discrete
#Obstacles = odds of obstructed point on network map

# <headingcell level=3>

# Import Data from GIS Source

# <codecell>

fromFile = False

#Extract list from matrix
def extract(matrix,minV,maxV):
    listed = []
    for x in range(len(matrix)):
        for y in range(len(matrix[0])):
            if matrix[x][y]>minV and matrix[x][y]<maxV:
                listed.append([x,y])
    return listed        

#Import matrix from file
def importFile(ref,mapper = 'null',limitHigh=big,limitLow=-big):
    paramDict = dict()
    fileIn = open(ref)
    content = [line.replace('\n','').replace('\t',' ').replace('\r','').replace('  ',' ') for line in fileIn.readlines()]
    fileIn.close()
    dLen = max([len(line.split(' ')) for line in content])
    params = [entry for entry in content if len(entry.split(' ')) < (.5*dLen)]
    entries = [entry.split(' ') for entry in content if len(entry) >= (.5*dLen)]
    entries = [[entry for entry in row if entry != ''] for row in entries]
    for i in range(len(params)):
        entry = params[i].replace('\t',' ')
        while '  ' in entry:
            entry = entry.replace('  ', ' ')
        temp = entry.split(' ')
        try:
            paramDict[temp[0]] = float(temp[1])
        except:
            try:
                paramDict[temp[0]] = temp[1]
            except:
                None
    xLen = len(entries)
    yLen = len(entries[0])
    print 'x:',xLen,'y:',yLen
    print paramDict
    if mapper != 'null':
        try:
            entries = [[mapper[entry] for entry in row] for row in reversed(entries)]
        except:
            entries = [[mapper[float(entry)] for entry in row] for row in reversed(entries)]
    else:
        entries = [[float(entry) for entry in row] for row in reversed(entries)]
    if limitHigh != 'null':
        for x in range(xLen):
            for y in range(yLen):
                if entries[x][y]>=limitHigh or entries[x][y]<=limitLow:
                    entries[x][y] = 0
    return entries,paramDict


if cfg['resourceFile'] != False or cfg['populationFile'] != False or cfg['roadFile'] != False:
    fromFile = True
    roadMapper = {-9999.0:-1,
                  60:0.0536,
                  40:0.0803,
                  15:0.215,
                  5:0.645,
                  83:0.646,
                  165:0.325,
                  667:0.0804,
                  1000:0.0536}
    roads,temp = importFile(cfg['roadFile'],mapper=roadMapper)
    population,temp = importFile(cfg['populationFile'],limitLow=0)
    cfg['extras']['viable'],temp = importFile(cfg['exclusionFile'],limitLow=0,limitHigh=2)
    resourceList = []
    if cfg['resourceFile'] != False:
        resources,temp = importFile(cfg['resourceFile'],limitLow=1)
        for x in range(len(resources)):
            for y in range(len(resources[0])):
                if resources[x][y] != 0:
                    resourceList.append([x,y])
    print "Number of resources:",len(resourceList)
    cfg['xSize'] = xSize = len(population)
    cfg['ySize'] = ySize = len(population[0])
    cfg['geo'] = temp
    
print "From file:", fromFile

# <codecell>

print "Road pts:", len(extract(roads,0,2))
print "Population points:", len(extract(population,0,999999999999))

# <headingcell level=3>

# Filter lower 10% of population density

# <codecell>

if False:
    increment = 1000
    percentile = 0.90
    limit = 0
    print "Trimming population grid..."
    loaded,temp = importFile(cfg['populationFile'],limitLow=0)
    score = total = len(extract(loaded,limit,big))
    while float(score)/total >= percentile:
        limit += increment
        score = len(extract(loaded,limit,big))
        print "\tLimit:",limit,"Score:",score
        
    population,temp = importFile(cfg['populationFile'],limitLow=limit)
    del loaded
        
    print "Population grid trimmed from size %s by %s percent to %s" % (total,1-percentile,score)

# <headingcell level=3>

# Geospatial edge weighting via Vincenty Algorithm

# <codecell>

def getWeight(pt1,pt2):
    return vincenty(pt1,pt2).miles
                        
def ptToCoords(pt,cfg):
    x = pt[1]; y = pt[0]
    cellSize = cfg['geo']['cellsize']
    xP = cfg['geo']['xllcorner']+cellSize*x
    yP = cfg['geo']['yllcorner']+cellSize*y
    return [yP,xP]

def placeEdge(cfg,pt1,pt2,speed,network):
    distance = getWeight(ptToCoords(pt1,cfg),ptToCoords(pt2,cfg))
    time = distance/(cfg['walkingSpeed']/speed)
    if network != 'null':
        network.add_edge(str(pt1),str(pt2),weight = time)
        return network
    return time

# <codecell>

#Testing & Debugging output
def getMaxDist(cfg):
    cellsize = cfg['geo']['cellsize']
    x1 = cfg['geo']['xllcorner']
    y1 = cfg['geo']['yllcorner']
    x2 = xllcorner + cfg['geo']['ncols']*cellsize
    y2 = yllcorner + cfg['geo']['nrows']*cellsize
    dists = [great_circle((y1,x1),(y1+c,x1)).miles,
             great_circle((y1,x1),(y1,x1+c)).miles,
             great_circle((y2,x1),(y2-c,x1)).miles,
             great_circle((y2,x1),(y2,x1+c)).miles]
    maxima = max(dists)
    print "Corner distances found:",dists
    print "Callibration maxima:",maxima
    return maxima
    
#cfg['geo']['maxDist'] = getMaxDist(cfg)

print placeEdge(cfg,[0,0],[1,0],1,'null')
print placeEdge(cfg,[0,0],[0,1],1,'null')

# <headingcell level=3>

# Population, Obstacle, and Resource Location Generation

# <codecell>

cfg['extras']['permanent'] = []   
            
print "Extracting point lists"    
#Extracting point lists from matrices   
if cfg['travel'] == 'network' and fromFile:
    cfg['extras']['lakes'] = roads
    pLocs = population
    cfg['extras']['occupied'] = extract(pLocs,0,99999999)
    if cfg['resourceFile'] != False:
        cfg['extras']['permanent'] = extract(resources,0,999)

        
print "Extracting viable sites list"
if cfg['inCityD']:
    #cfg['extras']['postExclusion'] = [loc for loc in cfg['extras']['occupied'] if cfg['extras']['exclude'][loc[0]][loc[1]] == 1]
    cfg['extras']['postExclusion'] =  extract(cfg['extras']['viable'],0,2)        
        
        
#Removing inaccessible locations
if cfg['filterAccessible']:
    print "Population filtered by road accessibility"
    cfg['extras']['occupied'] = [i for i in cfg['extras']['occupied'] if cfg['extras']['lakes'][i[0]][i[1]] != -1]
#Add low speed roads to occupied pts
else:
    print "Speed 1 road network added to populated areas"
    count = 0
    for i in (cfg['extras']['occupied']+cfg['extras']['postExclusion']+cfg['extras']['permanent']):
        if cfg['extras']['lakes'][i[0]][i[1]] == -1:
            cfg['extras']['lakes'][i[0]][i[1]] = 1
            count += 1
    print count,"points added"
           
    
print "Extracting adjacency lists"
#Calculating neighbors if hill climber constrained to populated points    
if cfg['inCityH']:
    neighbors = []
    viableTemp = set([str(pt) for pt in cfg['extras']['viable']])
    for i in cfg['extras']['occupied']:
        if i in cfg['extras']['postExclusion']:
            d = cfg['wander'] + 1
            x = i[0]
            y = i[1]
            accessible = []
            for xP in range(x-d,x+d):
                for yP in range(y-d,y+d):
                    try:
                        if cfg['extras']['lakes'][xP][yP] != -1:
                            accessible.append([xP,yP])
                    except:
                        None
            accessibleTemp = set([str(pt) for pt in accessible])
            accessible = [listFromStr(pt) for pt in list(accessibleTemp.intersection(viableTemp))]
            #if cfg['exclusionFile'] != False:
                #accessible = [loc for loc in accessible if cfg['extras']['viable'][loc[0]][loc[1]] == 1]
        else:
            accessible = []
        neighbors.append(deepcopy(accessible))
        
    cfg['extras']['neighbors'] = neighbors

        
if cfg['resourceFile'] != False:
    print "Fixed resources:",extract(resources,0,999)
else:
    print "No fixed resources found"

# <codecell>

print "Road pts:", len(extract(cfg['extras']['lakes'] ,0,2))
print "Population points:", len(cfg['extras']['occupied'])
print "Outlying cities:", len(extract(pLocs,0,99999999999)) 

# <codecell>

#Scatters initial set of resources
rLocs = [[randint(0,cfg['xSize']-1),randint(0,cfg['ySize']-1)] for x in range(nResource)]

# <headingcell level=3>

# Network Population Functions

# <codecell>

#Generates ranked key for node name
def getKey(key1,key2):
    pt1 = key1
    pt2 = listFromStr(key2)
    return str(rank(pt1[0],pt1[1],pt2[0],pt2[1]))

#Converts key str back to list
def listFromStr(text):
    return [int(entry) for entry in list(text.replace('[','').replace(']','').replace('(','').replace(')','').split(','))]

#Grid distance
def distance(pt1,pt2):
    return sqrt(pow(pt1[0]-pt2[0],2)+pow(pt1[1]-pt2[1],2))

# <codecell>

#Checks if outlying pt has any adjacent points that are closer to the origin point
def noneClose(pt,listed,limit):
    for i in range(len(listed)):
        pair = listed[i]
        if distance(pt[0],pair[0]) < limit and pt[1]>=pair[1] and pt!=pair:
            return False
    return True

#Generates network from sparse raster
def connectNetwork(cfg):
    network = nx.Graph()
    roadPts = extract(cfg['extras']['lakes'],0,big)
    viableTemp = set([str(pt) for pt in roadPts])
    count = 0
    firstSearch = 12
    secondSearch = 4
    proxLimit = 2
    print "Preparing to connect sparse raster of size:", len(roadPts)
    for i in roadPts:
        #d = cfg['netRadius']
        d = 12
        x = i[0]
        y = i[1]
        accessible = []
        for xP in range(x-d,x+d):
            for yP in range(y-d,y+d):
                try:
                    if cfg['extras']['lakes'][xP][yP] != -1:
                        accessible.append([xP,yP])
                except:
                    None

        accessibleTemp = set([str(pt) for pt in accessible])
        accessibleTemp.remove(str(i))
        accessible = [listFromStr(pt) for pt in list(accessibleTemp.intersection(viableTemp))]
        
        if len(accessible) != 0:
        
            distances = []
            for pt in accessible:
                distances.append(distance(pt,[xP,yP]))
            
            keptDist = sorted(distances)[:min(firstSearch,len(distances))]
            cutOff = keptDist[-1]
            
            distPairs = [[accessible[j],distances[j]] for j in range(len(distances)) if distances[j] <= cutOff]
            distPairs2 = [pair for pair in distPairs if noneClose(pair,distPairs,proxLimit)]
            
            keptDist = sorted([pair[1] for pair in distPairs2])[:min(secondSearch,len(distPairs2))]
            
            try: #Take the 4 closest points that are not proximal to other closer points
                cutOff = keptDist[-1]
                accessible = [pair[0] for pair in distPairs2 if pair[1] <= cutOff]
            except: #If a star is detected, take the 4 closest points
                keptDist = sorted([pair[1] for pair in distPairs])[:min(secondSearch,len(distPairs))]
                cutOff = keptDist[-1]
                accessible = [pair[0] for pair in distPairs if pair[1] <= cutOff]
                
            for pt in accessible:
                weight = max([cfg['extras']['lakes'][pt[0]][pt[1]],cfg['extras']['lakes'][x][y]])
                network = placeEdge(cfg,pt,[x,y],weight,network)

            count += 1
            if count%5000 == 0:
                print count,len(accessible),
    
    print "Network generation complete, [x,y]:" ,cfg['xSize'],cfg['ySize']
    print "Nodes:", len(network)
    print "Edges:", network.number_of_edges()          
    return network

# <codecell>

def getNetwork(cfg):
    """Generates Network for Manhattan Distance Travel & 8 directional grid"""
    network=nx.Graph()
    try:
        xSize = cfg['xSize']
    except:
        xSize = size
    try:
        ySize = cfg['ySize']
    except:
        ySize = size

    for x in range(xSize-1):
        for y in range(ySize):
            if cfg['extras']['lakes'][x+1][y] != -1 and cfg['extras']['lakes'][x][y] != -1:
                weight = max([cfg['extras']['lakes'][x+1][y],cfg['extras']['lakes'][x][y]])
                network = placeEdge(cfg,[x+1,y],[x,y],weight,network)

    for x in range(xSize):
        for y in range(ySize-1):
            if cfg['extras']['lakes'][x][y+1] != -1 and cfg['extras']['lakes'][x][y] != -1:
                weight = max([cfg['extras']['lakes'][x][y+1],cfg['extras']['lakes'][x][y]])
                network = placeEdge(cfg,[x,y+1],[x,y],weight,network)

                
    roadV = cfg['roadLimit']
    for x in range(xSize-1):
        for y in range(ySize-1):
            if cfg['extras']['lakes'][x][y] != -1 and cfg['extras']['lakes'][x+1][y+1] != -1:
                if cfg['extras']['lakes'][x][y] < roadV and cfg['extras']['lakes'][x+1][y+1] < roadV:
                    weight = max([cfg['extras']['lakes'][x][y],cfg['extras']['lakes'][x+1][y+1]])
                    network = placeEdge(cfg,[x,y],[x+1,y+1],weight,network)

    for x in range(1,xSize):
        for y in range(ySize-1):
            if cfg['extras']['lakes'][x][y] != -1 and cfg['extras']['lakes'][x-1][y+1] != -1: 
                if cfg['extras']['lakes'][x][y] < roadV and cfg['extras']['lakes'][x-1][y+1] < roadV:
                    weight = max([cfg['extras']['lakes'][x][y],cfg['extras']['lakes'][x-1][y+1]])
                    network = placeEdge(cfg,[x,y],[x-1,y+1],weight,network)

    print "Network generation complete, [x,y]:" ,cfg['xSize'],cfg['ySize']
    print "Nodes:", len(network)
    print "Edges:", network.number_of_edges()          
    return network

# <codecell>

if cfg['netMode'] == 'grid':
    cfg['extras']['network'] = getNetwork(cfg)
else:
    cfg['extras']['network'] = connectNetwork(cfg)

# <headingcell level=3>

# Sparsify function reduces resolution for nodes below a reachability threshold for high resolution runs

# <codecell>

def sparsify(network,cfg,pLocs,size):
    limit = cfg['sparsifyLimit']
    newNet = deepcopy(network)
    xSize = cfg['xSize']
    ySize = cfg['ySize']
    
    destinations = cfg['extras']['postExclusion'] + cfg['extras']['permanent']
    print "Number of destinations:",len(destinations)
    print "Limit:",limit
    xRemoved = 0; yRemoved = 0
    
    if cfg['sparsifyX']:
        for x in range(0,xSize-1,2):
            for y in range(ySize):
                if cfg['extras']['lakes'][x][y]>=limit and cfg['extras']['lakes'][x+1][y]>=limit:
                    if True:
                        newNet.remove_node(str([x+1,y]))
                        cfg['extras']['lakes'][x+1][y] = -1
                        pLocs[x][y] += pLocs[x+1][y]
                        pLocs[x+1][y] = 0
                        xRemoved += 1
                        #print "removing x node",x,y
                    else:
                        None
                    if True:
                        #print "adding x edge",x,y,x+2,y
                        weight = max(cfg['extras']['lakes'][x][y],cfg['extras']['lakes'][x+2][y])
                        newNet = placeEdge(cfg,[x,y],[x+2,y],weight,newNet)
                    else:
                        None
        print "X Sparsification complete, removed", xRemoved,"edges"
    if cfg['sparsifyY']:
        for x in range(xSize):
            for y in range(0,ySize-1,2):
                if cfg['extras']['lakes'][x][y]>=limit and cfg['extras']['lakes'][x][y+1]>=limit:
                    try:
                        newNet.remove_node(str([x,y+1]))
                        cfg['extras']['lakes'][x][y+1] = -1
                        pLocs[x][y] += pLocs[x][y+1]
                        pLocs[x][y+1] = 0
                        yRemoved += 1
                        #print "removing y node",x,y
                    except:
                        None
                    try:
                        #print "adding y edge",x,y,x,y+2
                        weight = max(cfg['extras']['lakes'][x][y],cfg['extras']['lakes'][x][y+2])
                        newNet = placeEdge(cfg,[x,y],[x,y+2],weight,newNet)
                    except:
                        None
        print "Y Sparsification complete, removed", yRemoved,"edges"
    occupied = extract(pLocs,0,99999)
    
    #Reinserting resources to network
    if xRemoved+yRemoved > 0:
        print "Re-adding resource edges to sparsified network"
        for resource in destinations:
            for x in range(resource[0]-1,resource[0]+1):
                for y in range(resource[1]-1,resource[1]+1):
                    if cfg['extras']['lakes'][x][y] != -1 and [x,y] != resource:
                        weight = min(cfg['extras']['lakes'][x][y],cfg['extras']['lakes'][resource[0]][resource[1]])
                        cfg['extras']['lakes'][x][y] = 1
                        newNet = placeEdge(cfg,[x,y],[resource[0],resource[1]],weight,newNet)
                        
    print "Network sparsification complete:",len(newNet),newNet.number_of_edges()          
    return newNet,pLocs,occupied,cfg['extras']['lakes']
            
if cfg['sparsifyY'] or cfg['sparsifyY']:      
    #cfg['extras']['network'],pLocs,cfg['extras']['occupied'],cfg['extras']['lakes'] = sparsify(cfg['extras']['network'],cfg,pLocs,0)
    temp,temp,temp,temp = sparsify(cfg['extras']['network'],cfg,pLocs,0)


# <headingcell level=3>

# Prioritizes Points to Reduce Redundancy

# <codecell>

def rank(x1,y1,x2,y2):
    if x1 < x2:
        return x1,y1,x2,y2
    elif x1 > x2:
        return x2,y2,x1,y1
    elif y1 < y2:
        return x1,y1,x2,y2
    else:
        return x2,y2,x1,y1

# <headingcell level=3>

# Reverse Lookup Method, find all distances to a give resource location

# <codecell>

inFile = open('DistDictNew.pickle','rb')
cfg['extras']['preCalculated'] = cPickle.load(inFile)
inFile.close()

# <codecell>

def reverseLookup(cfg,rLocs,pLocs,call='null'):
    try:
        distances = cfg['extras']['preCalculated']
        preCalc = True
    except:
        preCalc = False
        
    data = []; weights = []
    
    if preCalc:
        destStrings = [str(pt) for pt in (rLocs+cfg['extras']['permanent'])]
    if not preCalc:
        try:
            last = cfg['extras']['last']
        except:
            last = dict()
        distances = dict()
        destinations = rLocs+cfg['extras']['permanent']
        destStrings = []
        for pt in destinations:
            key = str(pt)
            try:
                distances[key] = last[key]
                destStrings.append(key)
            except:
                try:
                    distances[key] = nx.single_source_dijkstra_path_length(cfg['extras']['network'],key)
                    destStrings.append(key)
                except:
                    None

    if call == 'null':
        for pt in cfg['extras']['occupied']:
            trips = set()
            key = str(pt)
            for dest in destStrings:
                try:
                    trips.add(distances[dest][key])
                except:
                    None
            if len(trips) != 0:
                data.append(min(trips))
                weights.append(pLocs[pt[0]][pt[1]])
        if not preCalc:
            cfg['extras']['last'] = destinations
        return data, weights
    
    elif call == 'plot':
        distMap = zeros((xSize,ySize))
        ptRefs = dict()
        for pt in cfg['extras']['occupied']:
            trips = set()
            key = str(pt)
            for dest in destStrings:
                try:
                    trips.add(distances[dest][key])
                except:
                    None
            if len(trips) != 0:
                ptRefs[str(pt)] = min(trips)
        
        if not preCalc:
            cfg['extras']['last'] = destinations
        
        for key,value in ptRefs.iteritems():
            pt = listFromStr(key)
            distMap[pt[0]][pt[1]] = value
        return distMap

# <headingcell level=3>

# Worker Functions

# <codecell>

def getPts(locs):
    """Converts pt list to plotable format"""
    ptsX = [pt[0] for pt in locs]
    ptsY = [pt[1] for pt in locs]
    return ptsX,ptsY

# <codecell>

def getStats(pLocs,rLocs,cfg):
    """Return distance stats"""
    if cfg['threads'] != 1:
        mgr = Manager(); cfgP = mgr.dict(cfg)
        out_q = Queue()
        
        block = int(ceil(len(cfg['extras']['occupied'])/float(cfg['threads'])))
        processes = []; distances = []

        for i in range(cfg['threads']):
            p = Process(target = statsProc, args = (pLocs,rLocs,cfgP,i,cfg['extras']['occupied'][block*i:block*(i+1)],out_q))
            processes.append(p)
            p.start()
            merged = {}
        for i in range(cfg['threads']):
            merged.update(out_q.get())
        for p in processes:
            p.join()
        for i in range(cfg['threads']):
            distances += merged[i]
        cfg = dict(cfgP.items())
            
    else:
        if cfg['distMethod'] != 'reverse':
            distances = [getClosest(pt,rLocs,cfg) for pt in cfg['extras']['occupied']]
        else:
            distances,weights = reverseLookup(cfg,rLocs,pLocs)
        
    if cfg['population'] == 'discrete':
        return {'max':max(distances),
                'min':min(distances),
                'mean':mean(distances),
                'std':std(distances)}
    else:
        if cfg['distMethod'] != 'reverse':
            weights = [pLocs[pt[0]][pt[1]] for pt in cfg['extras']['occupied']]
        mean = average(distances, weights=weights)
        return {'max':max(distances),
                'min':min(distances),
                'mean':mean,
                'std':pow(average((distances-mean)**2, weights=weights),0.5)}

# <headingcell level=3>

# Plot Generation

# <codecell>

def pltGrid(pLocs,rLocs,cfg,stats='null',
            title='null',
            call='null',
            cmap ='Greys_r', 
            cmap2 = 'autumn_r',
            transpose = True,
            popAlpha = 1,
            alphaPow = 0.2,
            distMap = True):
    
    if call == 'animate' or call == 'final':
        plt.clf()
        spacer = '\n\n'
        xNudge = -.05
        yNudge = 0.87
        #fig = plt.figure(figsize=(15,15))
    else:
        fig = plt.figure(figsize=(15,15))
        spacer = ' '*7
        xNudge = -0.05
        yNudge = 1.005


    ax1 = plt.subplot(221)
    
    
    if cfg['travel'] == 'network':
        plt.imshow(cfg['extras']['lakes'],cmap=cmap,interpolation='nearest',alpha =0.7)
        plt.xlim(0,len(cfg['extras']['lakes'][0]))
        plt.ylim(0,len(cfg['extras']['lakes']))
    else:
        plt.xlim(0,max([entry[0] for entry in cfg['extra']['occupied']]))
        plt.ylim(0,max([entry[1] for entry in cfg['extra']['occupied']]))
    if cfg['population'] == 'discrete':
        pX,pY = getPts(pLocs)
        plt.scatter(pY,pX, s=2, c='black', alpha=0.25)
    elif call != 'final' and call != 'finalOut':
        colorized = mapColors(pLocs,1,cmap2)
        colorized = mapTransparency(pLocs,colorized,alphaPow)
        plt.imshow(colorized,cmap=cmap2,interpolation='nearest',zorder=2,alpha = popAlpha)
    if call != 'final':
        plt.title("Regional Population Density and Travel Network",size=12)
    else:
        plt.title("Regional Travel Network",size=12)
        
    xTicks = arange(0,cfg['geo']['ncols'],25)
    yTicks = arange(0,cfg['geo']['nrows'],25)
    xLabels = [str(ptToCoords([0,pt],cfg)[1])[0:7] for pt in xTicks]
    yLabels = [str(ptToCoords([pt,0],cfg)[0])[0:7] for pt in yTicks]
    plt.xticks(xTicks, xLabels,rotation=45,size=10)
    plt.yticks(yTicks, yLabels,size=10)
    plt.grid(True)
    
        
    rX,rY = getPts(rLocs)
    plt.scatter(rY,rX, s=250, zorder=3, c = 'none')
    plt.scatter(rY,rX, s=250, c='red', zorder=3, alpha = .5)
    
    bX,bY = getPts(cfg['extras']['permanent'])
    plt.scatter(bY,bX, s=250, zorder=3, c = 'none')
    plt.scatter(bY,bX, s=250, c='blue', zorder=3, alpha = .5)
    
    
    ax2 = plt.subplot(222)
    if distMap:
        plt.imshow(reverseLookup(cfg,rLocs,pLocs,call='plot'),origin='lower')
    plt.title("Expected Travel Time by Region",size = 12)
    plt.xticks(xTicks, xLabels,rotation=45,size=10)
    yLabels = ['' for pt in yTicks]
    plt.yticks(yTicks, yLabels,size=10)
    plt.grid(True)
    
    
    if call == 'final' or call == 'finalOut':
        for x, y, in zip(rY, rX):
            try:
                ax1.annotate(ptsToText([[x,y]],cfg,precision = 7)[0], 
                xy = (x, y), xytext = (-20, 20),
                textcoords = 'offset points', ha = 'left', va = 'bottom',
                bbox = dict(boxstyle = 'round,pad=0.4', fc = 'yellow', alpha = 0.8),
                arrowprops = dict(arrowstyle = '->', connectionstyle = 'arc3,rad=0'),
                zorder = 100)
                ax2.texts.append(ax1.texts.pop())
            except:
                None
            plt.suptitle('%s Resource Final Results' % nResource,fontsize =18,y=yNudge)
    
    if call != 'animate' and call != 'final':
        plt.tight_layout()
    else:
        None
        #plt.subplots_adjust(wspace=0.02)
    
    if title != 'null' and call != 'final' and call != 'finalOut':
        plt.suptitle(title,fontsize =18,y=yNudge)
    if stats != 'null':
        plt.xlabel('Mean Travel Time (hrs): %s%sMax Travel Time: %s%sStd Dev: %s' % (stats['mean'],spacer,stats['max'],spacer,stats['std']),
                   x=xNudge,size=14)
    if call != 'animate' and call != 'final':
        plt.show()
    return plt

# <codecell>

def pltSparse(pLocs,rLocs,cfg,stats='null',
            title='null',
            call='null',
            cmap ='Greys_r', 
            cmap2 = 'autumn_r',
            transpose = True,
            popAlpha = 1,
            alphaPow = 0.2,
            distMap = True):
    
    if call == 'animate' or call == 'final':
        plt.clf()
        spacer = '\n\n'
        xNudge = -.05
        yNudge = 0.87
        #fig = plt.figure(figsize=(15,15))
    else:
        fig = plt.figure(figsize=(15,15))
        spacer = ' '*7
        xNudge = -0.05
        yNudge = 1.005


    ax1 = plt.subplot(221)
    
    
    if cfg['travel'] == 'network':
        plt.imshow(cfg['extras']['lakes'],cmap=cmap,interpolation='nearest',alpha =0.7)
        plt.xlim(0,len(cfg['extras']['lakes'][0]))
        plt.ylim(0,len(cfg['extras']['lakes']))
    else:
        plt.xlim(0,max([entry[0] for entry in cfg['extra']['occupied']]))
        plt.ylim(0,max([entry[1] for entry in cfg['extra']['occupied']]))
    if cfg['population'] == 'discrete':
        pX,pY = getPts(pLocs)
        plt.scatter(pY,pX, s=2, c='black', alpha=0.25)
    elif call != 'final' and call != 'finalOut':
        colorized = mapColors(pLocs,1,cmap2)
        colorized = mapTransparency(pLocs,colorized,alphaPow)
        plt.imshow(colorized,cmap=cmap2,interpolation='nearest',zorder=2,alpha = popAlpha)
    if call != 'final':
        plt.title("Regional Population Density and Travel Network",size=12)
    else:
        plt.title("Regional Travel Network",size=12)
        
    xTicks = arange(0,cfg['geo']['ncols'],25)
    yTicks = arange(0,cfg['geo']['nrows'],25)
    xLabels = [str(ptToCoords([0,pt],cfg)[1])[0:7] for pt in xTicks]
    yLabels = [str(ptToCoords([pt,0],cfg)[0])[0:7] for pt in yTicks]
    plt.xticks(xTicks, xLabels,rotation=45,size=10)
    plt.yticks(yTicks, yLabels,size=10)
    plt.grid(True)
    
        
    rX,rY = getPts(rLocs)
    plt.scatter(rY,rX, s=250, zorder=3, c = 'none')
    plt.scatter(rY,rX, s=250, c='red', zorder=3, alpha = .5)
    
    bX,bY = getPts(cfg['extras']['permanent'])
    plt.scatter(bY,bX, s=250, zorder=3, c = 'none')
    plt.scatter(bY,bX, s=250, c='blue', zorder=3, alpha = .5)
    
    
    ax2 = plt.subplot(222)
    if distMap:
        plt.imshow(reverseLookup(cfg,rLocs,pLocs,call='plot'),origin='lower')
    plt.title("Expected Travel Time by Region",size = 12)
    plt.xticks(xTicks, xLabels,rotation=45,size=10)
    yLabels = ['' for pt in yTicks]
    plt.yticks(yTicks, yLabels,size=10)
    plt.grid(True)
    
    
    if call == 'final' or call == 'finalOut':
        for x, y, in zip(rY, rX):
            try:
                ax1.annotate(ptsToText([[x,y]],cfg,precision = 7)[0], 
                xy = (x, y), xytext = (-20, 20),
                textcoords = 'offset points', ha = 'left', va = 'bottom',
                bbox = dict(boxstyle = 'round,pad=0.4', fc = 'yellow', alpha = 0.8),
                arrowprops = dict(arrowstyle = '->', connectionstyle = 'arc3,rad=0'),
                zorder = 100)
                ax2.texts.append(ax1.texts.pop())
            except:
                None
            plt.suptitle('%s Resource Final Results' % nResource,fontsize =18,y=yNudge)
    
    if call != 'animate' and call != 'final':
        plt.tight_layout()
    else:
        None
        #plt.subplots_adjust(wspace=0.02)
    
    if title != 'null' and call != 'final' and call != 'finalOut':
        plt.suptitle(title,fontsize =18,y=yNudge)
    if stats != 'null':
        plt.xlabel('Mean Travel Time (hrs): %s%sMax Travel Time: %s%sStd Dev: %s' % (stats['mean'],spacer,stats['max'],spacer,stats['std']),
                   x=xNudge,size=14)
    if call != 'animate' and call != 'final':
        plt.show()
    return plt

# <codecell>

def mapTransparency(grid,coloredGrid,alphaPow):
    temp = deepcopy(coloredGrid)
    maxVal = max([max(row) for row in grid])
    for i in range(len(temp)):
        for j in range(len(temp[0])):
            
            temp[i][j][3] = pow((float(grid[i][j])/maxVal),alphaPow)
    return temp


def mapColors(grid,level,cmap):
    cNorm = colors.Normalize(vmin=0, vmax=level)
    scalarMap = cm.ScalarMappable(norm=cNorm, cmap=cmap)
    if type(grid) is list:
        temp = array(grid).tolist()
    else:
        temp = grid.tolist()
    for x in range(len(grid)):
        for y in range(len(grid[0])):
            temp[x][y] = list(scalarMap.to_rgba(float(grid[x][y])))
    return temp


def ptsToText(pts,cfg,precision = 10):
    locStrings = []
    gCoder = geocoders.GoogleV3()
    for pt in pts:
        coord = ptToCoords(pt,cfg)
        try:
            place, (lat,lon) = gCoder.geocode("%s,%s" % (coord[0],coord[1]))
        except:
            place = 'Unknown Road'
        locStrings.append('%s : {%s, %s}' % (place,str(coord[0])[0:precision],str(coord[1])[0:precision]))
    return locStrings


def pltLocs(pLocs,rLocs,cfg,stats='null',
            title='null',
            call='null',
            cmap ='Greys_r', 
            cmap2 = 'autumn_r',
            transpose = True,
            popAlpha = 1,
            alphaPow = 0.2,
            distMap = True):
    
    if cfg['netMode'] == 'grid':
        print "Plotting grid map"
        return pltGrid(pLocs,rLocs,cfg,stats='null',
            title='null',
            call='null',
            cmap ='Greys_r', 
            cmap2 = 'autumn_r',
            transpose = True,
            popAlpha = 1,
            alphaPow = 0.2,
            distMap = True)
    elif cfg['netMode'] == 'proximity':
        print "Plotting sparse map"
        return pltSpares(pLocs,rLocs,cfg,stats='null',
            title='null',
            call='null',
            cmap ='Greys_r', 
            cmap2 = 'autumn_r',
            transpose = True,
            popAlpha = 1,
            alphaPow = 0.2,
            distMap = True)

# <headingcell level=3>

# Starting Conditions

# <codecell>

"""stats  = {'mean':0,'max':0,'std':0}
pltLocs(pLocs,rLocs,cfg,title = 'Starting Conditions',popAlpha = 1, alphaPow = .25, stats= stats)"""

# <headingcell level=3>

# Denaturing Algorithm

# <codecell>

def denature(pLocs,rLocs,meanTilt,cfg,
             tracker=[],
             stopLimit = 50,
             timeLimit = 20000,
             showEvery = 25,
             best = 10000000,
             bestStats=dict(),
             show = True):
    
    """Randomly replaces 1 to n/2 resource locations and keeps if new solution scores better"""
    n = len(rLocs)
    gen = 0
    scores = []
    bestStats = dict()
    t1 = datetime.datetime.now()
    while len(scores)<stopLimit or len(set(scores[-stopLimit:])) != 1:
        newLocs = deepcopy(rLocs)
        ids = sample(range(n),randint(1,int(n/2.+.5)))
        for x in ids:
            if cfg['inCityD']:
                loc = choice(cfg['extras']['postExclusion'])
            else:
                loc = [randint(0,cfg['xSize']-1),randint(0,cfg['ySize']-1)]
            newLocs[x] = loc
            
        try:
            stats = cfg['extras']['tried'][str(sorted(newLocs))]
        except:
            stats = getStats(pLocs,newLocs,cfg)
            cfg['extras']['tried'][str(sorted(newLocs))] = stats
            
        score = stats['mean']*meanTilt + stats['max']*(1-meanTilt)
        if best > score:
            rLocs = deepcopy(newLocs); bestStats = deepcopy(stats)
            best = score
            tracker.append({'gen':'Denature %s' % gen,'score':score,'stats':deepcopy(stats),'pts':deepcopy(rLocs)})
        if gen%showEvery == 0:
            print "Best: %s   Uni: %s   Gen: %s   Mean: %s   Max: %s   Std: %s   Time: %s" % (best,
                                                                   len(set(scores[-stopLimit:])),
                                                                   gen,bestStats['mean'],
                                                                   bestStats['max'],
                                                                   bestStats['std'],
                                                                   (datetime.datetime.now()-t1).seconds)
        if gen%(showEvery*20) == 0 and show:
            pltLocs(pLocs,rLocs,cfg,bestStats,title='Denaturer Gen %s Optimal Placement' % gen,cmap='Greys_r',cmap2='autumn_r')
        if gen == timeLimit:
            break
        scores.append(best)
        gen += 1
    if gen == timeLimit:
        print "\nGeneration limit reached after %s generations" % timeLimit
    else:
        print "\nStable solution found at generation %s and maintained for %s generations" % (gen-stopLimit,stopLimit)
    print len(tracker), "improvements made"
    return rLocs,tracker,best,bestStats


# Experiment Results

#Denature until stable solution is maintained for 30000 generations
stagnantLimit = 2000
best = 10000000
showEvery = 100
cfg['threads'] = 1

rLocs,tracker,best,bestStats = denature(pLocs,rLocs,meanTilt,cfg,
                              stopLimit = stagnantLimit,
                              timeLimit = 'null',
                              showEvery = showEvery,
                              best = best,
                              show = False)



do = 'save'
try:
        fileOut = open('Tracker%s.pickle' % nResource,'w')
        cPickle.dump(tracker,fileOut)
        fileOut.close()
        print "Saved tracker to file"
except:
    none
    


outFile = open('ResourceLocations%s.text' % nResource,'w')
for location in sorted(ptsToText(rLocs,cfg,precision = 10)):
    outFile.write(location)
outFile.close()


# <rawcell>

# © 2014 James Schlitt

