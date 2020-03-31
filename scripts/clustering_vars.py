from pymongo import MongoClient, DESCENDING
from pymongo.errors import ServerSelectionTimeoutError
import os

vectorizedFeaturesColumn = 'features'
scaledFeaturesColumn = 'scaledFeatures'
predictionColumn = 'prediction'
mongoURL = (lambda: os.getenv('MONGO_URL', 'mongodb://localhost:27017/'))
dbName = 'clusterDatabase'
kCollectionName = 'kCollection'
originalCollectionName = 'originalCollection'
d3CollectionName = 'd3Collection'
confCollectionName = 'confCollection'
listSuffix = '_List'
idSuffix = '_id'
limitSuffix = '_limit'
intervalPrefix = 'daily'

defaultConf = {
	'args' : { 
		'daily' : {
			'limit': 100
		},
		'weekly' : {
			'limit': 200
		},
		'monthly' : {
			'limit': 300
		},
		'yearly' : {
			'limit': 400
		}
	},
	'optimalKarg' : 'monthly',
	'fileName' : 'data.csv',
	'hdfsURL' : 'hdfs://clustering_hdfs:9000',
	'hdfsFolder' : '/user/clustering/',
	'startK' : 2,
	'stopK' : 10,
	'iterNum' : 3,
	'thresholdedIterNum': 6,
	'threadNum' : 12,
	'silhouetteThreshold' : 0.9,
	'oldSilhouetteThreshold': 0.8,
	'd3NormalizeMax': 50,
	'png' : False,
	'algorithms' : [ 
		'KMeans'
	],
	'filteringColumns' : {
		'customer_id' : [ 
			'application_id'
		]
	},
	'columns' : {
		'cpu_percent' : [ 
			'ram_usage'
		]
	},
	'dontScale' : [
		'cpu_percent'
	]
}

configuration = None

def setConf():

	global configuration

	try:
		mongoClient = MongoClient(mongoURL())
		db = mongoClient[dbName]
		confCollection = db[confCollectionName]
		confRes = confCollection.find_one(sort=[('_id', DESCENDING)])
	except ServerSelectionTimeoutError as e:
		print('>>>[ERROR] MongoDB server ({}) cannot be reached!'.format(mongoURL()))
		print('MongoDB Server error: ', e)
	except Exception as e:
		print('>>>[ERROR]: ', e)

		print(defaultConf)
		return defaultConf

	if confRes is not None:
		objID = confRes.pop('_id')
	else:
		print('Configuration not found!')
		return defaultConf

	configuration = dict(defaultConf)

	for key in confRes.keys():
		configuration[key] = confRes[key]

	# for key in defaultConf.keys():
	# 	if confRes.get(key) is None:
	# 		confRes[key] = defaultConf[key]

	print(configuration)

	return configuration

def getConf():
	if configuration is None:
		return defaultConf
	return configuration
