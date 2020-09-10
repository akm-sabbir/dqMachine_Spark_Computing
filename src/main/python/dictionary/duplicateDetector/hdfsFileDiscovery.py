#from hdfs 
import sys
import datetime
import os
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark import SparkConf
import datetime
from collections import defaultdict
from operator import itemgetter,attrgetter
def initSystem(sc = None):
	#conf = SparkConf()
	#sc = SparkContext(conf = conf)
	URI = sc._gateway.jvm.java.net.URI
	Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
	fileStatus = sc._gateway.jvm.org.apache.hadoop.fs.FileStatus
	FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
	Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
	conf = Configuration()
	distributedSys = sc._gateway.jvm.org.apache.hadoop.hdfs.DistributedFileSystem
	localfileSys = sc._gateway.jvm.org.apache.hadoop.fs.LocalFileSystem
	conf.set("fs.defaultFS","hdfs://lpwhdqnnp01.npd.com")
	#conf.set("fs.hdfs.impl",distributedSys())
	#conf.set("fs.file.impl",localfileSys())
	#val output = fs.create(new Path("/npd/test/ODS/fileName"))
	fs = FileSystem.get(URI("hdfs://lpwhdqnnp01.npd.com"), Configuration())
	#path = Path("/npd/ODS/ODS_INPUTS_BZ2/ODS_POSOUTLETITEMS/")

	return fs,Path
def rangeOfdate(numdays):
	base = datetime.datetime.today()
	date_list = [base - datetime.timedelta(days = x) for x in xrange(0, numdays)]
	date_list = [str(each.year)+str(each.month)+ str(each.day) for each in date_list]
	return date_list
def todaysKey(hr = 0):
	dt = datetime.datetime.today()
	if hr is 0:
		return str(dt.year) + str(dt.month) + str(dt.day)
	else:
		return str(dt.year) + str(dt.month) + str(dt.day) + str(dt.hour) 
def globalMain(pathName = '/npd/ODS/ODS_INPUTS_BZ2/ODS_POSOUTLETITEMS/',sc_ = None):
	fs,Path = initSystem(sc_)  #FileSystem.get(URI("hdfs://lpwhdqnnp01.npd.com"), Configuration())
	path = Path(pathName)
	groupedDict = defaultdict(list)
	try:
		status = fs.listStatus(path)
		print len(status)
		fileLists = []
		if len(status) is not 0:
			for ind in xrange(0,len(status)):
				#print str(status[ind].getPath()) + " " + datetime.datetime.fromtimestamp(long(str(status[ind].getModificationTime()))/1000.).strftime('%m-%d-%Y %H:%M:%S')
				fileLists.append((str(status[ind].getPath()),datetime.datetime.fromtimestamp(long(str(status[ind].getModificationTime()))/1000.)))
		fileLists = sorted(fileLists, key = itemgetter(0),reverse = True)
		print fileLists[0][1].strftime('%m-%d-%Y %H:%M:%S')	
		for each in fileLists:
			#print str(each[1].year)+str(each[1].month)+str(each[1].day)+str(each[1].hour)
			groupedDict[str(each[1].year)+str(each[1].month)+str(each[1].day)+str(each[1].hour)].append(each[0])
		for key,values in groupedDict.iteritems():
			print str(key) + ' ' + str(values)
		#print "Path deleting in process ..."
		#fs.delete(Path('/npd/test/s_test/hiveUdf'))
	except ( IOError or ValueError or OSError or TypeError) as e:
		print e
	#except :
		print 
		print "generic exception"
	return groupedDict
#print ("End of the programe execution")
def deletePath(pathName = None,sc_ = None):
	if pathName is not None:
		fs,Path = initSystem(sc_)
		try:
			fs.delete(Path(pathName))
			return True
		except:
			print ("Java IO exception could not delete")
			return False
	return False
#globalMain()
