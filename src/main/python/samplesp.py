from pyspark import SparkContext
from utils import isParsedtoDouble, oneHotVector, computeMapper,conv2rows

sc = SparkContext("local", "App Name", "utils.py")

# To achieve one hot encoding we need to know the number of elements 
# within in each categorical feature which is obtained by grouping
# all the data by columns 


def getArrayLines(sc):
	filename  = "hdfs://priya-Aspire-V3-571:54310/tmp/bank.csv" 
	data = sc.textFile(filename)
	header = data.take(1)
	rows = data.filter(lambda line: line != header[0]) # Filtering out header
 	lineArr = rows.map(lambda line: line.split(";")).map(lambda line : zip(xrange(len(line)),line) ) # Tuples of (columnidx, value)
	return lineArr 


lineArr = getArrayLines(sc)
allWordtups = lineArr.flatMap(lambda y: y) # Flattering out the entire row
result = allWordtups.groupBy(lambda y: y[0]).cache() # Grouping all items by the columns
categItems = result.filter( lambda x: not isParsedtoDouble(x[1]))  # Find out which columsn are categorical

# Set Operation on the grouped categorical column which gives us the number of
# classes that particula categorical feature takes on  
categSet = categItems.map(lambda x: (x[0], set(x[1].__iter__() ) )).collect()
categDict = {}
for ky, vals in categSet:
	categDict[ky] = [y for x, y in vals ]

# Compute the mapper for each class of the categorical feature
mapdict =   computeMapper(categDict)
ll = lineArr.map( lambda row: conv2rows(row, mapdict) )
outdir = "hdfs://priya-Aspire-V3-571:54310/tmp/rowdir2" 
ll.saveAsTextFile(outdir)
