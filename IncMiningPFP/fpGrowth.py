'''FP-Growth'''
import header as myHeader
import tree as myTree
import sys

#----------scan the db-----------
def scanDB(path, separation):
	db = []
	f = open(path, 'r')
	for line in f:
		if line:
			db.append(line.rstrip().split(separation))
	f.close()
	return db

#-----------get item counts for a dataset---------
def getDBItems(db, basePtn=''):
	dbItems = {}
	newDB = []
	if basePtn:
		for trx in db:
			index = trx.index(basePtn)
			newDB.append(trx[:index + 1])
			for item in trx[:-1]:
				dbItems[item] = dbItems.get(item, 0) + 1
	else:
		for trx in db:
			for item in trx:
				dbItems[item] = dbItems.get(item, 0) + 1
	return dbItems, newDB

#-----------build an fp-tree-----------
def buildFPTree(db, dbItems, minsup):
	fpTree = myTree.FPTree()
	fpTree.createHeaderTable(dbItems, minsup)
	for trx in db:
		fpTree.add(trx, 1)
	return fpTree

#-----------get item counts for a pattern base-----------
def getPBItems(pb):
	pbItems = {}
	for ptn in pb:
		for item in ptn[0]:
			pbItems[item] = pbItems.get(item, 0) + ptn[1]
	return pbItems

#-----------build a conditional fp-tree-----------
def buildCondTree(condPB, minsup):
	condTree = myTree.FPTree()
	pbItems = getPBItems(condPB)
	condTree.createHeaderTable(pbItems, minsup)
	for ptn in condPB:
		condTree.add(ptn[0], ptn[1])
	return condTree

#-----------mine an fp-tree for a pattern-----------
def mine(tree, header, basePtn, minsup):
	basePtn += header._key + ','
	ptr = header._next
	# count = 0
	condPB = []
	while ptr:
		# count += ptr._count
		ptn = tree.prefix_path(ptr)
		if ptn:
			condPB.append(ptn)
		ptr = ptr._next
	patterns = [basePtn]
	if len(condPB) > 0:
		condTree = buildCondTree(condPB, minsup)
		patterns += mineAll(condTree, minsup, basePtn)
	return patterns

#-----------mine an fp-tree-----------
def mineAll(tree, minsup, basePtn='', incFlist=[]):
	allPatterns = []
	for header in tree.headerTable.headers():
		if incFlist:
			if header._key in incFlist:
				allPatterns += mine(tree, header, basePtn, minsup)
		else:
			allPatterns += mine(tree, header, basePtn, minsup)
	# sort
	for i in range(len(allPatterns)):
		tempPtn = sorted(allPatterns[i].rstrip(",").split(","))
		allPatterns[i] = ",".join(tempPtn)
	return allPatterns


def buildAndMine(gid, db, minsup, basePtn=''):
	dbItems, newDB = getDBItems(db)
	if newDB:
		fpTree = buildFPTree(newDB, dbItems, minsup)
	else:
		fpTree = buildFPTree(db, dbItems, minsup)
	results = mineAll(fpTree, minsup, basePtn)

	#resRDD = sc.parallelize(results)
	#resRDD.saveAsTextFile(resultPath + "_" + str(gid) + ".txt")
	#
	# with open(resultPath + "_" + str(gid) + ".txt", 'w') as outputf:
	#     outputf.write("\n".join(results))

	return results


#-----------mine trx-----------
def mineAllTrx(tree, incFlist):
	db = []
	for header in tree.headerTable.reverse_headers():
		if header._key in incFlist:
			ptr = header._next
			while ptr:
				trx = tree.upward_branch_traversal(ptr)
				if trx:
					db.append(trx)
				ptr = ptr._next
	return db


def getIncDBItems(db, incFlist=[]):
	dbItems = {}
	for trx in db:
		if not incFlist:
			for item in trx:
				dbItems[item] = dbItems.get(item, 0) + 1
		else:
			for item in trx[0]:
				dbItems[item] = dbItems.get(item, 0) + int(trx[1])
	return dbItems

#------
def constructIncDB(incFlist, newDB, minsup):
    incDBItems = getIncDBItems(newDB)
    fpTree = buildFPTree(newDB, incDBItems, minsup)
    incDB = mineAllTrx(fpTree, incFlist)
    return incDB

#------
def buildIncFPTree(db, dbItems, minsup):
	fpTree = myTree.FPTree()
	fpTree.createHeaderTable(dbItems, minsup)
	for trx in db:
		fpTree.add(trx[0], int(trx[1]))
	return fpTree

def buildAndMineIncDB(incFlist, incDB, minsup, basePtn=''):
    incDBItems = getIncDBItems(incDB, incFlist)
    fpTree = buildIncFPTree(incDB, incDBItems, minsup)
    results = mineAll(fpTree, minsup, basePtn, incFlist)
    return results


def checkBuildAndMine(oldResults, incFlist, gItems, gid, db, minsup, basePtn=''):
	# check if the group is affected
	dontSkip = False
	for item in gItems:
		if item in incFlist:
			dontSkip = True
			break
	# if yes, get the group-dependent incDB and mine
	if dontSkip:
		incDB = constructIncDB(incFlist, db, minsup)
		results = buildAndMineIncDB(incFlist, incDB, minsup, basePtn)

		mergedResults = []
		for item in results:
			if item not in oldResults:
				mergedResults.append(item)
		mergedResults.extend(oldResults)

		# resRDD = sc.parallelize(mergedResults)
		# resRDD.saveAsTextFile(resultPath + "_" + str(gid) + ".txt")

		# with open(resultPath + "_" + str(gid) + ".txt", 'w') as outputf:
		# 	outputf.write("\n".join(mergedResults))

		return mergedResults
	return False
