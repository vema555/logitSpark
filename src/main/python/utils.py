## Helper functios for computing 
## 1) The one hot encoder for categorical iterms
## 2) Covnverting this row of categorical and quantitateive features into quantFeatures

def isParsedtoDouble(xiter):
	""" Returbs True if Quantitative Variable otherwise returns Flase for categorical  """ 
	xit = xiter.__iter__()
	xit.next()
	ky, x = xit.next()
 	try:
		float(x)
 	except:
		return False
	return True


# Building a one Hot Vector
def oneHotVector(N, i):
	x = [0] *(N-1)
	if i != (N-1):
 		x[i] = 1
	return x


def computeMapper(categDict):
	#Compute the Mapper:
	mapdict = {}
	for ky, clist in categDict.iteritems():
		mapdict[ky] = {}
		N = len(clist)
		zr = [0] * N
		for i, category in enumerate(clist):
	 		mapdict[ky][category] = oneHotVector(N, i)
	return mapdict

def conv2rows(row, mapdict):
	""" Map the row to arrays """
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
