from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, Vector, DenseVector, VectorUDT
from pyspark.sql.functions import lit
# from pyspark.sql.functions import udf, monotonically_increasing_id, max as spark_max
from pymongo import MongoClient
from math import sqrt
from clustering_vars import vectorizedFeaturesColumn, scaledFeaturesColumn
from clustering_vars import listSuffix, limitSuffix
from clustering_vars import getConf


def createSpark():
	"""
	Creates a SparkSession object.

	Creates or gets already existing Spark Session from Spark Engine.

	Parameters:
	None

	Returns:
	SparkSession object
	"""

	sparkSession = SparkSession.builder.getOrCreate()
	return sparkSession


def readData(dataFileName, sparkSession, limit):
	"""
	Reads data from a CSV file.

	Reads data from a CSV file with column names and proper type, converts data to Spark DataFrame.

	Parameters:
	dataFileName (str): CSV file name
	header (bool): Determines if the first line is header line
	inferSchme (bool): Determines the type of the data read

	Returns:
	Spark DataFrame object
	"""

	hdfsURL = getConf()['hdfsURL']
	hdfsFolder = getConf()['hdfsFolder']

	try:
		sc = SparkContext.getOrCreate()
		URI = sc._gateway.jvm.java.net.URI
		Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
		FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
		fs = FileSystem.get(URI(hdfsURL), sc._jsc.hadoopConfiguration())

		if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(hdfsFolder + dataFileName)):
			df = sparkSession.read.csv(hdfsURL + hdfsFolder + dataFileName, header=True, inferSchema=True)
			# df = df.withColumn('UID', monotonically_increasing_id())
			dfCount = df.count()
			if dfCount > limit:
				fraction = limit / dfCount
				df = df.sample(fraction)
				print(df.count())

			return df
		else:
			print('>>> File not found!')
			return None
	except Exception as e:
		print('>>>[ERROR] HDFS connection error: ', e)
		raise e

def scale(df, clusteringColumns):
	"""
	Scales given vector column.

	Scales given vector column between 0 and 100.

	Parameters:
	df (DataFrame): Spark DataFrame object

	Returns:
	DataFrame with scaled column added
	"""

	dontScale = getConf()['dontScale']
	scalingColumns = list()
	dfColumns = df.columns
	scaledDF = df.na.drop(subset=clusteringColumns)

	for column in clusteringColumns:
		prop = column[:column.index('_')]
		if column in dontScale:
			scalingColumns.append(column)
		elif prop + limitSuffix in dfColumns:
			scaledDF = scaledDF.withColumn(prop + '_scaled', (scaledDF[column] * 100) / scaledDF[prop + limitSuffix])
			scalingColumns.append(prop + '_scaled')
		else:
			colMax = scaledDF.agg({column: 'max'}).collect()[0][0]
			colMin = scaledDF.agg({column: 'min'}).collect()[0][0]

			if (colMax - colMin) > 0:
				scaledDF = scaledDF.withColumn(prop + '_scaled', (scaledDF[column] - colMin) / (colMax - colMin) * 100)
			else:
				scaledDF = scaledDF.withColumn(prop + '_scaled', lit(0.0))
			scalingColumns.append(prop + '_scaled')

	assembler = VectorAssembler(inputCols=scalingColumns, outputCol=scaledFeaturesColumn)
	assembledData = assembler.transform(scaledDF)

	return assembledData


def macroDFs(df, macroColumn, microColumn):
	"""
	Filters data for given macro filter name.

	The first step of filtering, filters the whole DataFrame for each unique member of given column.

	Parameters:
	df (DataFrame): Spark DataFrame object
	macroColumn (str): Column name for macro filtering
	microColumn (str): Column name for micro filtering

	Returns:
	Dictonary with macro segments in it
	"""

	distinctValues = df.select(macroColumn).distinct().collect()
	distinctValues = [row[macroColumn] for row in distinctValues]

	macroDFList = list()

	for value in distinctValues:
		macroDict = {macroColumn: value, (microColumn+listSuffix): df.where(df[macroColumn] == value)}
		macroDFList.append(macroDict)

	return macroDFList


def filteredDFs(macroDFList, microColumn):
	"""
	Filters data for given micro filter name.

	The second step of filtering, filters the each macro segmented DataFrame for each unique member of micro column.

	Parameters:
	macroDFList (dict): Dictonary with macro segments in it
	microColumn (str): Column name for micro filtering

	Returns:
	Dictionary of dictionaries with micro segmented data in it
	"""

	dfList = list(macroDFList)

	for macroIndex, macroDFDict in enumerate(macroDFList):
		macroDF = macroDFDict[(microColumn+listSuffix)]

		distinctValues = macroDF.select(microColumn).distinct().collect()
		distinctValues = [row[microColumn] for row in distinctValues]

		microDFList = list()

		for value in distinctValues:
			microDict = {microColumn: value, 'DF': macroDF.where(macroDF[microColumn] == value)}
			microDFList.append(microDict)
		dfList[macroIndex][(microColumn+listSuffix)] = list(microDFList)

	return dfList


def distanceToLine(point, linePoint1, linePoint2):
	x0, y0 = point

	x1, y1 = linePoint1
	x2, y2 = linePoint2

	# slope
	a = -(float(y2 - y1) / float(x2 - x1))
	# y coefficient
	b = 1
	# line constant
	c = -y1 + (-a*x1)

	distance = abs(a*x0 + b*y0 + c) / sqrt(a**2 + b**2)

	return distance
