import re 
from elasticsearch import Elasticsearch 
import sys 
import glob 
from os import listdir 
import urllib2
import json
from datetime import datetime

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

#songName = raw_input("Please enter the name of the song : ")
#print qry

songLy = raw_input("Please enter any word in the song : ")


res = es.search(index="music",size=2, body={"query": {"match_all":{}}}) #{'lyrics': songLy}}})

print("%d documents found" % res['hits']['total'])
docsID = []
for doc in res['hits']['hits']:
	docsID.append(doc["_id"])
	print(doc["_source"])

"""
print("searching...")
res = es.search(index = indexName, size=2, body={"query": {"match_all": {}}})
print(" response: '%s'" % (res))

print("results:")
for hit in res['hits']['hits']:
    print(hit["_source"])

"""
