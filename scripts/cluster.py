from pyspark import SparkContext
from pyspark.sql import SparkSession, Column
from pyspark.ml.clustering import KMeans, BisectingKMeans, GaussianMixture
# import numpy as np
from pymongo import MongoClient, DESCENDING
from pymongo.errors import ServerSelectionTimeoutError
from clustering_vars import predictionColumn, vectorizedFeaturesColumn, scaledFeaturesColumn
from clustering_vars import mongoURL, dbName, kCollectionName, originalCollectionName, d3CollectionName
from clustering_vars import listSuffix, idSuffix, limitSuffix, intervalPrefix
from clustering_vars import getConf, setConf
from metrics import clusterRadius, gaussianCenters, entropy, getSilhouette, scaleD3
from optimal_k import optimalModel, thresholdedOptimalModel
from optimal_k import updateMicroK, updateMacroK
from preprocess import createSpark, readData, scale, macroDFs, filteredDFs
import json, time, concurrent.futures, argparse, os
from datetime import date


def getKList(xColumn, yColumn, macroColumn, microColumn, algName):
	try:
		mongoClient = MongoClient(mongoURL())
		db = mongoClient[dbName]
		kCollection = db[kCollectionName]

		q = {'algorithm': algName, 'macro': macroColumn, 'micro': microColumn, 'firstColumn': xColumn, 'secondColumn': yColumn}
		# print(q)
		res = kCollection.find_one(q, sort=[('_id', DESCENDING)])
	except ServerSelectionTimeoutError as e:
		print('>>>[ERROR] MongoDB server ({}) cannot be reached!'.format(mongoURL()))
		print('>>>[ERROR] MongoDB Server error: ', e)
	except Exception as e:
		print('>>>[ERROR]: ', e)

		return None

	if res is not None:
		objID = res.pop('_id')
		kDict = res['list']
		return kDict
	else:
		return None


def kClustering(df, algorithm, k, oldSilhouette=None):
	"""
	Separates data in clusters.

	Separates data in clusters with optimal cluster number for given algorithm.

	Parameters:
	df (DataFrame): Spark DataFrame for clustering
	algorithm(function): Spark algorithm for clustering
	k (int): Cluster count

	Returns:
	Optimal model for given algorithm and clustered DataFrame
	"""
	if oldSilhouette is None:
		clusteringModel, optSeed = optimalModel(df, k, algorithm)
	else:
		clusteringModel, optSeed = thresholdedOptimalModel(df, k, algorithm, oldSilhouette)

	# clusteringAlg = algorithm(featuresCol=scaledFeaturesColumn, k=k).setSeed(seed)
	# clusteringModel = clusteringAlg.fit(df)
	clusteredData = clusteringModel.transform(df)
	silhouette = getSilhouette(clusteredData)

	#pndDF = clusteredData.toPandas()
	#pndDF.plot(kind='scatter', x=xColumn, y=yColumn, colormap='tab20', colorbar=True, c='prediction').get_figure().savefig(algName + '-'  + str(macroColumn) + '-' + str(microColumn) + '-' + str(xColumn) + '-' + str(yColumn) + '.png')

	return clusteringModel, clusteredData, silhouette


def createClusters(dfList, macroColumn, microColumn, xColumn, yColumn, algName, algorithm, interval):
	print('>>> Thread started')
	"""
	Creates clusters with segmentation.

	Creates clusters with macro and micro segmentation.

	Parameters:
	**df (DataFrame): Spark DataFrame for clustering and segmentation
	macroColumn (str): Column name for macro segmentation
	microColumn (str): Column name for micro segmentation
	xColumn (str): First column name for clustering
	yColumn (str): Second column name for clustering
	algName (str): Algorithm name for creating png files
	algorithm(function): Spark algorithm for clustering

	Returns:
	List of macro segment dictionaries with micro segment lists in it, micro segment lists is list of dictionaries with cluster lists in it
	"""

	silhouetteThreshold = getConf()['oldSilhouetteThreshold']

	clusterList = list(dfList)

	kDict = getKList(xColumn, yColumn, macroColumn, microColumn, algName)
	if kDict is None:
		print('>>>[ERROR] kDict is None!')
		return

	for macroIndex, macro in enumerate(dfList):
		macroID = macro[macroColumn]
		if kDict.get(macroID) is None:
			macroKDict = updateMacroK(macro, macroID, macroColumn, microColumn, xColumn, yColumn, algName, algorithm)
			if macroKDict is None:
				continue
			kDict[macroID] = macroKDict

		for microIndex, micro in enumerate(macro[(microColumn+listSuffix)]):
			microID = micro[microColumn]
			segmentedDF = micro.pop('DF')

			if segmentedDF.select([xColumn, yColumn]).distinct().count() >= 2:
				scaledData = scale(segmentedDF, [xColumn, yColumn])
			else:
				print('>>> Distinct < 2')
				continue

			optDict = kDict[macroID].get(microID)
			if optDict is None:
				k, oldSilhouette = updateMicroK(segmentedDF, macroID, microID, macroColumn, microColumn, xColumn, yColumn, algName, algorithm)
				if (k is None) or (oldSilhouette is None):
					continue
			else:
				k = optDict['k']
				oldSilhouette = optDict['silhouette']

			model, clusteredDF, silhouette = kClustering(scaledData, algorithm, k, oldSilhouette)
			print('>>>', silhouette, oldSilhouette)

			if silhouette < oldSilhouette * silhouetteThreshold:
				print('>>> Recalculating k ', macroID, microID, xColumn, yColumn, algName)
				k, oldSilhouette = updateMicroK(segmentedDF, macroID, microID, macroColumn, microColumn, xColumn, yColumn, algName, algorithm)
				model, clusteredDF, silhouette = kClustering(scaledData, algorithm, k)

			if getConf()['png']:
				folderDir = 'ClusterPNGs/{}/{}/{}/{}/{}/{}/'.format(intervalPrefix, str(date.today()), macroColumn, microColumn, macroID, microID)
				if not os.path.isdir(folderDir):
					os.makedirs(folderDir)
				pndDF = clusteredDF.toPandas()
				pndDF.plot(kind='scatter', x=xColumn, y=yColumn, colormap='tab20', colorbar=True, c='prediction').get_figure().savefig(os.path.join(folderDir, '{}-{}-{}.png'.format(xColumn, yColumn, algName)))

			clusterList[macroIndex][(microColumn + listSuffix)][microIndex]['entropy'] = entropy(clusteredDF, 2)
			clusterList[macroIndex][(microColumn + listSuffix)][microIndex]['silhouette'] = silhouette

			clusterNames = clusteredDF.select(predictionColumn).distinct().collect()
			clusterNames = [row[predictionColumn] for row in clusterNames]
			clusterNames.sort()

			if algorithm == GaussianMixture:
				centerList = gaussianCenters(clusteredDF, clusterNames)
			else:
				centerList = model.clusterCenters()

			clusterList[macroIndex][(microColumn+listSuffix)][microIndex]['clusters'] = list()
			for (clusterIndex, clusterName), center in zip(enumerate(clusterNames), centerList):
				clusterDF = clusteredDF.where(clusteredDF[predictionColumn] == clusterName)

				radius = clusterRadius(clusterDF, center)
				# intraCluster = intraClusterDistance(clusterDF)
				clusterInfo = {'name': clusterName, 'center': center.tolist(), 'clusterSize': clusterDF.count(), 'radius': radius}#, 'silhouette': silhouette}#, 'intraClusterDistance': intraCluster, 'interClusterDistances': dict()}
				clusterList[macroIndex][(microColumn+listSuffix)][microIndex]['clusters'].append(clusterInfo)

				# for innerClusterName in clusterNames:
				# 	if clusterName != innerClusterName:
				# 		innerClusterDF = clusteredDF.where(clusteredDF[predictionColumn] == innerClusterName)
				# 		interDistance = interClusterDistance(clusterDF, innerClusterDF)
				# 		clusterList[macroIndex][(microColumn+listSuffix)][microIndex]['clusters'][clusterIndex]['interClusterDistances'][str(clusterName) + '-' + str(innerClusterName)] = interDistance

	insertToMongo(clusterList, macroColumn, microColumn, xColumn, yColumn, algName, interval)
	return clusterList


def insertToMongo(clusterList, macroColumn, microColumn, xColumn, yColumn, algName, interval):
	"""
	Inserts created results from clustering analysis.

	Inserts 2 documents, first one is untouched version of result, second one is for visualizing.

	Parameters:
	clusterList (list): createClusters function's return value
	macroColumn (str): Column name for macro segmentation
	microColumn (str): Column name for micro segmentation
	xColumn (str): First column name for clustering
	yColumn (str): Second column name for clustering
	algName (str): Algorithm name for creating png files

	Returns:
	None
	"""

	try:
		todayStr = str(date.today())

		mongoDict = {'algorithm': algName, 'macro': macroColumn, 'micro': microColumn, 'firstColumn': xColumn, 'secondColumn': yColumn, 'date': todayStr, 'list': clusterList}
		print('---------------')
		print(mongoDict)
		print('---------------')

		d3Json = json.dumps(clusterList)
		d3Json = d3Json.replace(macroColumn, 'name')
		d3Json = d3Json.replace((microColumn + listSuffix), 'children')
		d3Json = d3Json.replace(microColumn, 'name')
		d3Json = d3Json.replace('clusters', 'children')
		# d3Json = d3Json.replace('radius', 'size')
		d3List = json.loads(d3Json)

		scaledD3List = scaleD3(d3List)
		d3Dict = {'name': 'clusters', 'children': scaledD3List, 'algorithm': algName, 'macro': macroColumn, 'micro': microColumn, 'firstColumn': xColumn, 'secondColumn': yColumn, 'date': todayStr}

		mongoClient = MongoClient(mongoURL())
		db = mongoClient[dbName]
		origCollection = db['{}_{}'.format(interval, originalCollectionName)]
		d3Collection = db['{}_{}'.format(interval, d3CollectionName)]

		origResult = origCollection.insert_one(mongoDict)
		d3Result = d3Collection.insert_one(d3Dict)

		if origResult is None:
			print('>>>[ERROR] Cannot insert mongoDict to originalCollection!')
		else:
			print('>>> ID: ', origResult.inserted_id)

		if d3Result is None:
			print('>>>[ERROR] Cannot insert d3Dict result to d3Collection!')
		else:
			print('>>> ID: ', d3Result.inserted_id)

	except ServerSelectionTimeoutError as e:
		print('>>>[ERROR] MongoDB server ({}) cannot be reached!'.format(mongoURL()))
		print('>>>[ERROR] MongoDB Server error: ', e)
	except Exception as e:
		print('>>>[ERROR]: ', e)


def main():
	# Main program flow

	parser = argparse.ArgumentParser()
	parser.add_argument('timeInterval', help='Choose a time interval e.g: (daily, weekly, monthly, yearly)')
	args = parser.parse_args()

	availableAlgorithms = {'KMeans': KMeans, 'BisectingKMeans': BisectingKMeans, 'GaussianMixture': GaussianMixture}
	algorithmDict = dict()

	conf = setConf()

	hdfsURL = conf['hdfsURL']
	hdfsFolder = conf['hdfsFolder']

	if args.timeInterval in conf['args'].keys():
		intervalPrefix = args.timeInterval

	for algName, algorithm in availableAlgorithms.items():
		if algName in conf['algorithms']:
			algorithmDict[algName] = algorithm

	spark = createSpark()
	# Data file name can be changed
	data = readData('{}_{}'.format(intervalPrefix, conf['fileName']), spark, conf['args'][intervalPrefix]['limit'])
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
						for algName,algorithm in algorithmDict.items():
							dfList = filteredDFs(macroDFs(data, macroColumn, microColumn), microColumn)
							executor.submit(createClusters, dfList, macroColumn, microColumn, xColumn, yColumn, algName, algorithm, intervalPrefix)
							# print(f.result())
							# cList = createClusters(dfList, macroColumn, microColumn, 'scaledFeatures', xColumn, yColumn, algName, algorithm)
							# insertToMongo(cList, macroColumn, microColumn, xColumn, yColumn, algName)
							print('>>>', macroColumn, microColumn, xColumn, yColumn, algName)

	duration = time.time() - start_time
	print(f">>> Finished in {duration} seconds")

	try:
		sc = SparkContext.getOrCreate()
		URI = sc._gateway.jvm.java.net.URI
		Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
		FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
		fs = FileSystem.get(URI(hdfsURL), sc._jsc.hadoopConfiguration())

		if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(hdfsFolder + '{}_{}'.format(intervalPrefix, conf['fileName']))):
			if fs.delete(Path(hdfsFolder + '{}_{}'.format(intervalPrefix, conf['fileName'])), False):
				print('>>> File deleted!')
			else:
				print('>>> File cannot be deleted!')
		else:
			print('>>> File not found!')
	except Exception as e:
		print('>>>[ERROR] HDFS error: ', e)
		raise e


if __name__ == '__main__':
	main()
