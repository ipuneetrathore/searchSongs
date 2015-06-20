import glob
from elasticsearch import Elasticsearch
import urllib2
import re
import pydoop.hdfs as hdfs


EsHost = {
    "host" : "localhost", 
    "port" : 9200
}

HDFSfiles=[]
for hdFiles in hdfs.ls("/gaana/gaanaLyrics"): #"gaanaLyrics/gaanaLyrics"):
	HDFSfiles.append(hdFiles[41:])


fileNames = []

indexName = 'music'
typeName = 'songs'
#IdField = 'songID'


bulkData = [] 

i = 1
for name in HDFSfiles:	
	dataDict = {}
       	fopen=hdfs.open("/gaana/gaanaLyrics/"+name)
	header = fopen.read()
       	header = re.sub('[^a-zA-Z]', ' ', header)
	header = header.replace("Advertisements"," ")
       	header = ''.join([item.lower() for item in header]) 
	songAndMovie = []
       	dlim = "lyrics"
#	nameNew = name.replace("-"," ")
	songAndMovie.append(name)
        dataDict[name] = header
	metaDict = {}
	dataDict = {}	
       	for elements in songAndMovie:
               	songsName = []
#              	if "lyrics" in elements:
		songName = elements.split('-')
		songName = songName[0]
		songsName.append(songName)
               	for itms in songsName:
			metaDict = {                             
                       		"index" : {
                               		'_index':indexName,
                               		'_type':typeName,
                               		'_id':i
                               		}
				}
			i  = i+1
			print i
				                            
#              	else:	
#			metaDict={
#                	"index" : {
#                       		'_index':indexName,
#                       		'_type':typeName, 
#                                '_id':i    
#                                   }
#
#                       		}
#			i = i+1
#			print i
			
			dataDict[i-1] = header+"Songname : ",songName	
			bulkData.append(metaDict)
                	bulkData.append(dataDict)

                       	

es = Elasticsearch(hosts = [EsHost],timeout=90)

                
if es.indices.exists(indexName):
	print("deleting '%s' index..." % (indexName))
	res = es.indices.delete(index = indexName)
	print(" response: '%s'" % (res))

request_body = {
    "settings" : {
        "number_of_shards": 2,
        "number_of_replicas": 1
    }
}

print("creating '%s' index..." % (indexName))
res = es.indices.create(index = indexName, body = request_body)
print(" response: '%s'" % (res))


# bulk index the data
print("bulk indexing...")
res = es.bulk(index = indexName, body = bulkData, refresh = True)
#print(" response: '%s'" % (res))

# sanity check
print("searching...")
res = es.search(index = indexName, size=2, body={"query": {"match_all": {}}})
print(" response: '%s'" % (res))

print("results:")
for hit in res['hits']['hits']:
    print(hit["_source"])
 


