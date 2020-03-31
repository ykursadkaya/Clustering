from pymongo import MongoClient, DESCENDING
import os

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

mongoURL = (lambda: os.getenv('MONGO_URL', 'mongodb://localhost:27017/'))
dbName = 'clusterDatabase'
confCollectionName = 'confCollection'


try:
	mongoClient = MongoClient(mongoURL())
	db = mongoClient[dbName]
	confCollection = db[confCollectionName]
	confRes = confCollection.insert_one(defaultConf)

	if confRes is None:
		print('>>>[ERROR] Cannot insert confrest to confCollection!')

except ServerSelectionTimeoutError as e:
	print('>>>[ERROR] MongoDB server ({}) cannot be reached!'.format(mongoURL()))
	print('MongoDB Server error: ', e)
except Exception as e:
	print('>>>[ERROR]: ', e)
