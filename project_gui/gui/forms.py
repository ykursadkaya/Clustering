from django import forms
from pymongo import MongoClient
import os

class SelectFile(forms.Form):
    file = forms.FileField(label='Select File ')


class ColumnSelection(forms.Form):
    macroList = list()
    microList = list()
    firstColumnList = list()
    secondColumnList = list()

    mongoClient = MongoClient(os.getenv('MONGO_URL', 'mongodb://localhost:27017/'))
    db = mongoClient['clusterDatabase']
    d3Collection = db['daily_d3Collection']

    macroList = d3Collection.distinct('macro')
    microList = d3Collection.distinct('micro')
    firstColumnList = d3Collection.distinct('firstColumn')
    secondColumnList = d3Collection.distinct('secondColumn')
    algorithmList = d3Collection.distinct('algorithm')

    macroTList = [tuple([str(c),str(c)]) for c in macroList]
    microTList = [tuple([str(c),str(c)]) for c in microList]
    firstColumnTList = [tuple([str(c),str(c)]) for c in firstColumnList]
    secondColumnTList = [tuple([str(c),str(c)]) for c in secondColumnList]
    algorithmTList = [tuple([str(c),str(c)]) for c in algorithmList]

    algorithmName = forms.CharField(label='Algorithm: ', widget=forms.Select(choices=algorithmTList))
    macroFilter = forms.CharField(label='Macro Filter Column: ', widget=forms.Select(choices=macroTList))
    microFilter = forms.CharField(label='Micro Filter Column: ', widget=forms.Select(choices=microTList))
    firstColumn = forms.CharField(label='Cluster column 1: ', widget=forms.Select(choices=firstColumnTList))
    secondColumn = forms.CharField(label='Cluster column 2: ', widget=forms.Select(choices=secondColumnTList))
