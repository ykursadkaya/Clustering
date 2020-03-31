from pyspark.sql import SparkSession, Column
from pyspark.ml.clustering import KMeans, BisectingKMeans, GaussianMixture
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import numpy as np
from clustering_vars import predictionColumn, vectorizedFeaturesColumn, scaledFeaturesColumn
from clustering_vars import mongoURL, dbName, kCollectionName
from clustering_vars import listSuffix, idSuffix, limitSuffix
from clustering_vars import getConf, setConf
from metrics import getSilhouette
from preprocess import createSpark, readData, scale, macroDFs, filteredDFs, distanceToLine
import concurrent.futures, time
from datetime import date


def optimalModel(df, k, algorithm):
	"""
	Finds optimal seed for given k.

	Finds optimal staring points for given k and algorithm via running same algorithm multiple times with different starting points.

	Parameters:
	df (DataFrame): Spark DataFrame for clustering
	k (int): Number of clusters
	algorithm(function): Spark algorithm for clustering

	Returns:
	Optimal seed for given k and algorithm
	"""

	iterNum = getConf()['iterNum']
	silPrev = -1
	optSeed = 0
	optModel = None
	seed = 0

	for i in range(0,iterNum):
		seed = np.random.randint(1, high=2147483647)
		currentAlg = algorithm(featuresCol=scaledFeaturesColumn, k=k).setSeed(seed)
		model = currentAlg.fit(df)
		#wssse = model.computeCost(df)
		predictions = model.transform(df)
		silhouette = getSilhouette(predictions)

		print('>>> ', i, silhouette)

		if silhouette > silPrev:
			silPrev = silhouette
			optSeed = seed
			optModel = model

	# while (iterNum>0):
	# 	seed = np.random.randint(1, high=1000)
	# 	currentAlg = algorithm(featuresCol=scaledFeaturesColumn, k=k).setSeed(seed)
	# 	model = currentAlg.fit(df)
	# 	#wssse = model.computeCost(df)
	# 	predictions = model.transform(df)
	# 	silhouette = 0

	# 	clusterNames = predictions.select(predictionColumn).distinct().collect()
	# 	if len(clusterNames) > 1:
	# 		evaluator = ClusteringEvaluator()
	# 		silhouette = evaluator.evaluate(predictions)

	# 	if silhouette > silPrev:
	# 		silPrev = silhouette
	# 		optSeed = seed
	# 		optModel = model
	# 	#else:
	# 	iterNum -= 1

	return optModel, optSeed


def thresholdedOptimalModel(df, k, algorithm, oldSilhouette):
	iterNum = getConf()['thresholdedIterNum']
	threshold = getConf()['oldSilhouetteThreshold']
	silPrev = -1
	optSeed = 0
	optModel = None
	seed = 0
	silhouette = 0

	while ((iterNum > 0) and (silhouette < oldSilhouette * threshold)):
		seed = np.random.randint(1, high=2147483647)
		currentAlg = algorithm(featuresCol=scaledFeaturesColumn, k=k).setSeed(seed)
		model = currentAlg.fit(df)
		#wssse = model.computeCost(df)
		predictions = model.transform(df)
		silhouette = getSilhouette(predictions)

		print('>>> ', iterNum, silhouette)

		if silhouette > silPrev:
			silPrev = silhouette
			optSeed = seed
			optModel = model
		iterNum -= 1

	return optModel, optSeed


def createGraphs(df, algorithm):
	"""
	Creates graphs for determining optimal K for given algorithm

	Calculates optimal seed, SSE value and Silhouette score for given algorithm between start and stop k values.

	Parameters:
	df (DataFrame): Spark DataFrame for clustering
	algorithm(function): Spark algorithm for clustering

	Returns:
	Dictonary with keys k and values seed
	Dictonary with keys k and values SSE
	List of dictonaries keys k and values Silhouette scores
	"""

	startK = getConf()['startK']
	stopK = getConf()['stopK']
	pointsDict = dict()
	silhouetteList = list()

	for k in range(startK,stopK+1):
		optModel, optSeed = optimalModel(df, k, algorithm)
		# clusteringAlg = algorithm(featuresCol=scaledFeaturesColumn, k=k).setSeed(optSeed)
		# kthModel = clusteringAlg.fit(df)

		if algorithm != GaussianMixture:
			kthwssse = optModel.computeCost(df)
			pointsDict[k] = kthwssse

		predictions = optModel.transform(df)

		kthSilhouette = getSilhouette(predictions)
		silhouetteList.append({'k': k, 'silhouette': kthSilhouette})

	return pointsDict, silhouetteList


def optimalK(pointsDict, silhouetteList, algorithm):
	"""
	Finds optimal k for given algorithm.

	Finds optimal k for given algorithm with comparing silhouette scores and SSE values.

	Parameters:
	pointsDict (dict): Dictonary with keys k and values SSE
	silhouetteList (list): List of dictonaries keys k and values Silhouette scores
	algorithm(function): Spark algorithm for clustering

	Returns:
	Optimal k and seed for given algorithm
	"""

	silhouetteThreshold = getConf()['silhouetteThreshold']
	startK = getConf()['startK']
	stopK = getConf()['stopK']

	silhouetteList.sort(key=lambda d: d['silhouette'])
	silhouetteList.reverse()
	bestSilhouettes = silhouetteList[0:3]

	print(silhouetteList)

	if algorithm != GaussianMixture:
		distanceDict = dict()
		for k, wssse in pointsDict.items():
			distanceDict[k] = distanceToLine((k, wssse), (startK, pointsDict[startK]), (stopK, pointsDict[stopK]))

		optKDict = {}

		if bestSilhouettes[0]['silhouette'] * silhouetteThreshold > bestSilhouettes[1]['silhouette']:
			optKDict = bestSilhouettes[0]
		else:
			if distanceDict[bestSilhouettes[1]['k']] > distanceDict[bestSilhouettes[0]['k']]:
				optKDict = bestSilhouettes[1]
			else:
				optKDict = bestSilhouettes[0]

		if optKDict['silhouette'] * silhouetteThreshold < bestSilhouettes[2]['silhouette']:
			if distanceDict[bestSilhouettes[2]['k']] > distanceDict[optKDict['k']]:
				optKDict = bestSilhouettes[2]

		optK = optKDict['k']
		optSilhouette = optKDict['silhouette']

	else:
		optK = silhouetteList[0]['k']
		optSilhouette = silhouetteList[0]['silhouette']

	return optK, optSilhouette


def insertToMongo(kDict, macroColumn, microColumn, xColumn, yColumn, algName):
	"""
	Inserts created results from clustering analysis.

	Inserts 2 documents, first one is untouched version of result, second one is for visualizing.

	Parameters:
	**clusterList (list): createClusters function's return value
	macroColumn (str): Column name for macro segmentation
	microColumn (str): Column name for micro segmentation
	xColumn (str): First column name for clustering
	yColumn (str): Second column name for clustering
	algName (str): Algorithm name for creating png files

	Returns:
	None
	"""

	# mongoDict = {'algorithm': algName, 'macro': macroColumn, 'micro': microColumn, 'firstColumn': xColumn, 'secondColumn': yColumn, 'list': kDict}

	try:
		query = {'algorithm': algName, 'macro': macroColumn, 'micro': microColumn, 'firstColumn': xColumn, 'secondColumn': yColumn}
		replacement = {'$set': {'list': kDict}}

		mongoClient = MongoClient(mongoURL())
		db = mongoClient[dbName]
		kCollection = db[kCollectionName]

		kResult = kCollection.update_one(query, replacement, upsert=True)

		if kResult is None:
			print('>>>[ERROR] Cannot insert  kResult to kCollection!')
		else:
			print('>>> ID: ', kResult.upserted_id)

	except ServerSelectionTimeoutError as e:
		print('>>>[ERROR] MongoDB server ({}) cannot be reached!'.format(mongoURL()))
		print('>>>[ERROR] MongoDB Server error: ', e)
	except Exception as e:
		print('>>>[ERROR]: ', e)


def updateMicroKOnMongo(macroID, microID, macroColumn, microColumn, xColumn, yColumn, algName, k, silhouette):

	try:
		todayStr = str(date.today())

		query = {'algorithm': algName, 'macro': macroColumn, 'micro': microColumn, 'firstColumn': xColumn, 'secondColumn': yColumn}
		update = {'$set': {'list.{}.{}'.format(macroID, microID): {'k': k, 'silhouette': silhouette, 'date': todayStr}}}

		mongoClient = MongoClient(mongoURL())
		db = mongoClient[dbName]
		kCollection = db[kCollectionName]

		kResult = kCollection.update_one(query, update, upsert=True)

		if kResult is None:
			print('>>>[ERROR] Cannot insert  kResult to kCollection!')
		else:
			print('>>> ID: ', kResult.upserted_id)

	except ServerSelectionTimeoutError as e:
		print('>>>[ERROR] MongoDB server ({}) cannot be reached!'.format(mongoURL()))
		print('>>>[ERROR] MongoDB Server error: ', e)
	except Exception as e:
		print('>>>[ERROR]: ', e)


def updateMacroKOnMongo(macroID, microID, macroColumn, microColumn, xColumn, yColumn, algName, macroKDict):
	try:
		query = {'algorithm': algName, 'macro': macroColumn, 'micro': microColumn, 'firstColumn': xColumn, 'secondColumn': yColumn}
		update = {'$set': {'list.' + macroID: macroKDict}}

		mongoClient = MongoClient(mongoURL())
		db = mongoClient[dbName]
		kCollection = db[kCollectionName]

		kResult = kCollection.update_one(query, update, upsert=True)

		if kResult is None:
			print('>>>[ERROR] Cannot insert  kResult to kCollection!')
		else:
			print('>>> ID: ', kResult.upserted_id)

	except ServerSelectionTimeoutError as e:
		print('>>>[ERROR] MongoDB server ({}) cannot be reached!'.format(mongoURL()))
		print('>>>[ERROR] MongoDB Server error: ', e)
	except Exception as e:
		print('>>>[ERROR]: ', e)

def saveK(dfList, macroColumn, microColumn, xColumn, yColumn, algName, algorithm):
	print('>>>> Thread started')
	todayStr = str(date.today())

	kDict = dict() 
	for macroList in dfList:
		macroID = macroList[macroColumn]
		kDict[macroID] = dict()
		for microList in macroList[(microColumn+listSuffix)]:
			microID = microList[microColumn]
			segmentedDF = microList.pop('DF')

			if segmentedDF.select([xColumn, yColumn]).distinct().count() >= 2:
				scaledData = scale(segmentedDF, [xColumn, yColumn])
				pointsDict, silhouetteList = createGraphs(scaledData, algorithm)
				k, silhouette = optimalK(pointsDict, silhouetteList, algorithm)
				print('>>> ',macroID, microID, str(k))
				kDict[macroID][microID] = {'k': k, 'silhouette': silhouette, 'date': todayStr}
			else:
				print('>>> Distinct < 2')
				continue

	if kDict is not None:
		insertToMongo(kDict, macroColumn, microColumn, xColumn, yColumn, algName)
		print('>>>', macroColumn, microColumn, xColumn, yColumn, algName)
	else:
		return


def updateMicroK(df, macroID, microID, macroColumn, microColumn, xColumn, yColumn, algName, algorithm):
	if df.select([xColumn, yColumn]).distinct().count() >= 2:
		scaledData = scale(df, [xColumn, yColumn])
		pointsDict, silhouetteList = createGraphs(scaledData, algorithm)
		k, silhouette = optimalK(pointsDict, silhouetteList, algorithm)
		print('>>>', macroID, microID, str(k))
		updateMicroKOnMongo(macroID, microID, macroColumn, microColumn, xColumn, yColumn, algName, k, silhouette)
		return k, silhouette
	else:
		print('>>> Distinct < 2')
		return None, None


def updateMacroK(macroDFList, macroID, macroColumn, microColumn, xColumn, yColumn, algName, algorithm):
	macroKDict = dict()
	todayStr = str(date.today())

	for microIndex, micro in enumerate(macroDFList[(microColumn+listSuffix)]):
		microID = micro[microColumn]
		segmentedDF = micro.get('DF')

		if segmentedDF.select([xColumn, yColumn]).distinct().count() >= 2:
			scaledData = scale(segmentedDF, [xColumn, yColumn])
			pointsDict, silhouetteList = createGraphs(scaledData, algorithm)
			k, silhouette = optimalK(pointsDict, silhouetteList, algorithm)
			macroKDict[microID] = {'k':k, 'silhouette': silhouette, 'date': todayStr}
			print('>>>', macroID, microID, str(k))
		else:
			print('>>> Distinct < 2')
			continue
	if macroKDict is not None:
		updateMacroKOnMongo(macroID, microID, macroColumn, microColumn, xColumn, yColumn, algName, macroKDict)
		return macroKDict
	else:
		return None


def main():
	availableAlgorithms = {'KMeans': KMeans, 'BisectingKMeans': BisectingKMeans, 'GaussianMixture': GaussianMixture}
	algorithmDict = dict()

	conf = setConf()

	defaultArg = conf['optimalKarg']

	# setGlobals(conf)

	for algName, algorithm in availableAlgorithms.items():
		if algName in conf['algorithms']:
			algorithmDict[algName] = algorithm

	spark = createSpark()
	# Data file name can be changed
	data = readData('{}_{}'.format(defaultArg, conf['fileName']), spark, conf['args'][defaultArg]['limit'])
	if data is None:
		return

	filterList = list()
	columnList = list()
	for columnName in data.columns:
		if columnName[-3:] == idSuffix:
			filterList.append(columnName)
		else:
			columnList.append(columnName)

	start_time = time.time()

	with concurrent.futures.ThreadPoolExecutor(max_workers=conf['threadNum']) as executor:
		for macroColumn in conf['filteringColumns'].keys():
			for microColumn in conf['filteringColumns'][macroColumn]:
				for xColumn in conf['columns'].keys():
					for yColumn in conf['columns'][xColumn]:
						for algName, algorithm in algorithmDict.items():
							dfList = filteredDFs(macroDFs(data, macroColumn, microColumn), microColumn)
							# clusterList = list(dfList)
							executor.submit(saveK, dfList, macroColumn, microColumn, xColumn, yColumn, algName, algorithm)
							# print(f.result())

	duration = time.time() - start_time
	print(f">>> Finished in {duration} seconds")

if __name__ == '__main__':
	main()
