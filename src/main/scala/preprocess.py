from pyspark import  SparkContext

sc = SparkContext("local", "App Name")
filename  = "hdfs://priya-Aspire-V3-571:54310/tmp/bank.csv" 
data = sc.textFile(filename)

header = data.take(1)
rows = data.filter(lambda line: line != header[0])

lineArr = rows.map(lambda line: line.split(";")).map(lambda line : zip(xrange(len(line)),line) )
allWordtups = lineArr.flatMap(lambda y: y)
result = allWordtups.groupBy(lambda y: y[0]).cache()
keys = sorted(result.keys().collect())
numcols = len(keys)

def f(index, iter):
    if index !=  0: return iter

#data.mapPartitionsWithIndex(lambda x,y : f(x, y)).collect()

#Now we have to figure out which one of these is convertibe to 
# a double and which one isnt 
colMap = {}

def isParsedtoDouble(xiter):
    xit = xiter.__iter__()
    xit.next()
    ky, x = xit.next()
    print ky, x
    try:
        float(x)
    except:
        return False
    return True

# All Categorical Columns
categItems = result.filter( lambda x: not isParsedtoDouble(x[1])) 
#
categSet = categItems.map(lambda x: (x[0], set(x[1].__iter__() ) )).collect()
categDict = {}
for ky, vals in categSet:
    categDict[ky] = [y for x, y in vals ]

#
def oneHotVector(N, i):
    x = [0] *(N-1)
    if i != (N-1):
        x[i] = 1
    return x

#Compute the Mapper:
mapdict = {}
for ky, clist in categDict.iteritems():
    mapdict[ky] = {}
    N = len(clist)
    zr = [0] * N
    for i, category in enumerate(clist):
        mapdict[ky][category] = oneHotVector(N, i)
 

def conv2rows(row, mapdict):
    prow = []
    for x, y in row:
        if mapdict.has_key(x) and mapdict[x].has_key(y):
            prow.append( mapdict[x][y] )
        elif mapdict.has_key(x):
            prow.append([y,])
        else:
            try:
                prow.append([float(y),])
            except:
                prow.append([y,])
    return [item for sublist in prow for item in sublist]
 
ll = lineArr.map( lambda row: conv2rows(row, mapdict) )
outfiledir  = "hdfs://priya-Aspire-V3-571:54310/tmp/preprocess" 
ll.saveAsTextFile(outfiledir)

 