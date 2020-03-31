from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, Vector, DenseVector, VectorUDT
from pyspark.ml.stat import Summarizer
from pyspark.sql import Column
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
from clustering_vars import predictionColumn, vectorizedFeaturesColumn, scaledFeaturesColumn, getConf
from math import log, sqrt


def clusterRadius(df, center):
	"""
	Calculates cluster radius.

	Calculates cluster radius via finding outermost point to cluster center.

	Parameters:
	df (DataFrame): Clustered Spark DataFrame
	center (Vector): Cluster center

	Returns:
	Cluster radius (float)
	"""

	distanceUDF = udf(lambda features: features.squared_distance(center).item(), FloatType())
	df = df.withColumn('distanceToCenter', distanceUDF(df[scaledFeaturesColumn]))

	return sqrt(df.agg({'distanceToCenter': 'max'}).collect()[0][0])


def gaussianCenters(df, clusterNames):
	"""
	Finds centers for GaussianMixture algorithm.

	Finds centers for GaussianMixture algorithm via calculating column mean for each cluster in given micro segment.

	Parameters:
	df (DataFrame): Clustered Spark DataFrame
	clusterNames (list): Cluster names in given micro segment

	Returns:
	Cluster centers (list of Vectors)
	"""

	summarizer = Summarizer.metrics("mean")
	centerList = list()

	meanColumn = 'mean({})'.format(scaledFeaturesColumn)
	for clusterID in clusterNames:
		center = df.where(df[predictionColumn] == clusterID).select(Summarizer.mean(df[scaledFeaturesColumn])).collect()[0][meanColumn]
		centerList.append(center)

	return centerList


def intraClusterDistance(df):
	"""
	Calculates average intra-cluster distance.

	Parameters:
	df (DataFrame): Clustered Spark DataFrame

	Returns:
	Intra-cluster distance for given cluster (float)
	"""

	rows = df.collect()
	size = df.count()

	distance = 0
	for index, firstRow in enumerate(rows):
		for secondRow in rows[index+1:]:
			distance += firstRow[scaledFeaturesColumn].squared_distance(secondRow[scaledFeaturesColumn])

	averageDistance = 0
	if size != 1:
		averageDistance = distance / (size * (size - 1))

	return averageDistance


def interClusterDistance(df1, df2):
	"""
	Calculates average inter-cluster distance for given clusters.

	Parameters:
	df1 (DataFrame): First clustered Spark DataFrame
	df2 (DataFrame): Second clustered Spark DataFrame

	Returns:
	Inter-cluster distance for given clusters (float)
	"""

	rows1 = df1.collect()
	rows2 = df2.collect()

	size1 = df1.count()
	size2 = df2.count()

	distance = 0
	for row1 in rows1:
		for row2 in rows2:
			distance += row1[scaledFeaturesColumn].squared_distance(row2[scaledFeaturesColumn])

	averageDistance = 0
	if size1 != 0 or size2 != 0:
		averageDistance = distance / (size1 * size2)

	return averageDistance


def entropy(df, base):
	"""
	Calculates entropy for given micro segment.

	Parameters:
	df (DataFrame): Clustered Spark DataFrame
	base (int): logarithm base
	predictionName (str): Created column name after clustering with cluster names

	Returns:
	Entropy value for given micro segment (float)
	"""

	clusterNames = df.select(predictionColumn).distinct().collect()
	clusterNames = [row[predictionColumn] for row in clusterNames]
	total = df.count()

	ent = 0
	for clusterName in clusterNames:
		clusterCount = df.where(df[predictionColumn] == clusterName).count()
		p = clusterCount / total
		ent += p * log(p, base)
	ent = -ent

	return ent


def getSilhouette(df):
	silhouette = 0

	clusterNames = df.select(predictionColumn).distinct().count()
	if clusterNames > 1:
		evaluator = ClusteringEvaluator(predictionCol=predictionColumn, featuresCol= scaledFeaturesColumn)
		silhouette = evaluator.evaluate(df)

	return silhouette


def scaleD3(d3List):
	radiusMin = float('inf')
	radiusMax = float('-inf')
	for customer in d3List:
		for application in customer['children']:
			for cluster in application['children']:
				if cluster['radius'] < radiusMin:
					radiusMin = cluster['radius']
				elif cluster['radius'] > radiusMax:
					radiusMax = cluster['radius']

	scaledD3List = list(d3List)
	for customer in scaledD3List:
		for application in customer['children']:
			for cluster in application['children']:
				if (radiusMax - radiusMin) <= 0 :
					cluster['size'] = 1
				else:
					cluster['size'] = (((cluster['radius'] - radiusMin) / (radiusMax - radiusMin)) * (getConf()['d3NormalizeMax'] - 1)) + 1

	return scaledD3List
