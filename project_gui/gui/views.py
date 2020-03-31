from django.shortcuts import render
from django.http import HttpResponse
from pymongo import MongoClient
import pymongo
from .forms import ColumnSelection
import json, os

# Create your views here.

def inputForm(request):
    if (request.method == 'POST'):
        form = ColumnSelection(request.POST)
        if form.is_valid():
            macroFilterName = form.cleaned_data['macroFilter']
            microFilterName = form.cleaned_data['microFilter']
            firstColumnName = form.cleaned_data['firstColumn']
            secondColumnName = form.cleaned_data['secondColumn']
            algName = form.cleaned_data['algorithmName']

            # print(macroFilterName)
            # print(microFilterName)
            # print(firstColumnName)
            # print(secondColumnName)

            mongoClient = MongoClient(os.getenv('MONGO_URL', 'mongodb://localhost:27017/'))
            db = mongoClient['clusterDatabase']
            d3Collection = db['daily_d3Collection']
            origCollection = db['daily_originalCollection']

            q = {'macro': macroFilterName, 'algorithm': algName, 'micro': microFilterName, 'firstColumn': firstColumnName, 'secondColumn': secondColumnName}
            res = d3Collection.find_one(q, sort=[('_id', pymongo.DESCENDING)])
            if res is None:
                q = {'macro': macroFilterName, 'algorithm': algName, 'micro': microFilterName, 'firstColumn': secondColumnName, 'secondColumn': firstColumnName}
                res = d3Collection.find_one(q, sort=[('_id', pymongo.DESCENDING)])
            if res is not None:
                mongo_id = res.pop('_id')
                jsonStr = json.dumps(res)

                # jsonFile = uploadFile.upload(jsonStr)
                # jsonFile = 'https://gist.githubusercontent.com/ykursadkaya/6bb00a65db23ee58c63b56f7795641f7/raw/0f0d0f3994d40127124aaad689b8e463d29b1bd9/gistfile1.txt'
                return render(request, 'clusterVis.html', {'jsonStr': jsonStr})
            else:
                print('Returned None from mongodb')
    else:
        form = ColumnSelection()
    return render(request, 'form.html', {'form': form})

# def handle_uploaded_file(f, fileName):
#     with open(fileName, 'wb+') as destination:
#         for chunk in f.chunks():
#             destination.write(chunk)
#     with open(fileName, 'r', encoding='utf-8-sig') as datasetFile:
#         headerLine = datasetFile.readline().replace('\n', '')
