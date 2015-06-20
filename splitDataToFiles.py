import glob
from elasticsearch import Elasticsearch
import urllib2
import re
import pydoop.hdfs as hdfs
#from subprocess import Popen, PIPE
from pywebhdfs.webhdfs import PyWebHdfsClient

hdfsFS = PyWebHdfsClient(host='localhost',port='50070', user_name='hduser')

fopen=hdfs.open("lyrics/part-m-00000")
data = fopen.read()

i=0
songs=[]
data = data.split('\n')
#tempList=[]
for Songs in data:
	tempList=[]
	song=Songs.split(',')
	for wds in song:
		tempList.append(wds)
	print i
	i=i+1
#o=1	
#for i in range(127):
#	print tempList[o]
#	o=o+4
	

	#fileToWrite = hdfs.open('gaana/lyrics/'+tempList[1], 'w')
	theFile=''.join('gaana/gaanaLyrics/'+tempList[1]+'-'+tempList[3])		 
	print theFile
	theData = ''.join(tempList[1]+','+tempList[2]+','+tempList[3])
	
	hdfsFS.create_file(theFile, theData)
	#fileToWrite.write(tempList[1]+'\n'+tempList[2]+'\n'+tempList[3])
	#fileToWrite.close()

print "Done."

