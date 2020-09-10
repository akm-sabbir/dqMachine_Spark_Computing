#!/usr/bin/env
import pickle
import pyspark
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
from pyspark import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import avg, udf, col
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from operator import add
import sys
from py4j.protocol import Py4JJavaError
import MySQLdb
from hdfsFileDiscovery import globalMain
from hdfsFileDiscovery import deletePath
from hdfsFileDiscovery import todaysKey
def quiet_logs(sc_ = None):
	logger = sc_._jvm.org.apache.log4j
	logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
	logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
	return
def hiveInit(sc_ = None):
	if  sc_ is None:
		print "Spark Context need to be initialized befor anything can be done"
		raise ValueError
	hive_context = HiveContext(sc_)	
	sql_context = SQLContext(sc_)
	return (hive_context, sql_context)
def listOffiles():
	#return ['./createExttabledictbase.sql','./createExternalTable.sql'] + 
	paths ='src/main/python/dictionary/fileSource/'
	subP1 = 'hivejoinScripts/'
	subP2 = 'hivecreateScripts/'
	subP3 ='hiveinsertScripts/'
	return [ paths + subP1 + 'removeDups.sql', paths + subP2 + 'createsnapshotsUnique.sql', paths + subP1 + 'retrieveBase.sql' ]#'performjoinUniquesnapshots.sql']	
def readFiles(fileName  = None):
	if fileName is None:
		print "file name is missing raising error and exiting"
		raise ValueError
	data = None
	with open(fileName) as dataReader:
		data = dataReader.readlines()
	return data
def hiveExecutecommands(line = None ,hive_context = None):
	rdd = None
	try:
		print "Executing command: " + str(line)
		rdd = hive_context.sql(line)
		#print "printing rdd: " + str(rdd)
	except (MySQLdb.Error, MySQLdb.Warning) as e:
		print ("exception MySqlDB")
		print (e)
	#	return None
	except NameError as e:
		print ("Exception as Name Error: " + str(e))
	except TypeError as e:
		print ("exception TypeError :")
		print (e)
	except ValueError as e:
		print ("something wrong with columns or conditions")
		print(e)
	finally:
		pass
	if rdd is not None:
		return rdd
	else:
		return None
	return 


def printwithstats(dataContainer):
	if isinstance(dataContainer, list) is True:
		print "length of the data container: " + str(len(dataContainer))
		print "printing  the content of the container"
		for each in dataContainer:
			if isinstance(each, tuple) is True:
				for datum in each[1]:
					print each[0] + " " + datum
			else:
				print each
	if isinstance(dataContainer,dict) is True:
		print "length of the data container: " + str(len(dataContainer))
		print "printing the content of the container"
		for each in dataContainer.iteritems():
			key,value = each
			if isinstance(value,list) is True:
				for each in value:
					print key + " " + each
	return

def executeScripts(fileName,hc,sqlc,spark):
	commands = readFiles(fileName = fileName)
	print "executing command: "  + str(commands)
	empty_dataframe = sqlc.createDataFrame(spark.emptyRDD(),StructType([]))
	for each in commands:
		data = hiveExecutecommands(line = each,hive_context = hc)
		if data.rdd.isEmpty() is not True and each.find("select") is not -1:
			if empty_dataframe.rdd.isEmpty() == True:
				empty_dataframe = data
			else:
				empty_dataframe = empty_dataframe.unionAll(data)
	return	empty_dataframe
def readrddFromcsvfile(filename = None,sqlContext = None):
    #global globalVar.spark_context	
    try:
        rdd1 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "false").option("delimiter","|").option("quote","\\").option("codec","bzip2").option("treatEmptyValuesAsNulls","true").load(filename)
        return rdd1
    except Py4JJavaError as e :
	print("invalid path name for data source")
	return None
    except (OSError,IOError, ValueError) as e :
        print e
        print('No file found '+str(ifile))
        return None
    except pyspark.sql.utils.AnalysisException as e:
	print e
	return None
    return  None

def readrddFromcsvfiles(filename = None,sqlContext = None):
    #global globalVar.spark_context	
    try:
        rdd1 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "false").option("delimiter","|").option("quote","\\").option("codec","bzip2").option("treatEmptyValuesAsNulls","true").load(filename + '/*')
        return rdd1
    except Py4JJavaError as e :
	print("invalid path name for data source" + str(e))
	return None
    except (OSError,IOError, ValueError) as e :
        print e
        print('No file found '+str(ifile))
        return None
    except pyspark.sql.utils.AnalysisException as e:
	print e
	return None
    return  None

def dofiltering(rdd,filtered):
	rdd = rdd.filter(rdd.itemid != filtered.itemid )
	return rdd
def writeDown(rddData,writingPath,append = 0, partitions = None):
	#if partitions is not None:
	#	rddData = rddData.repartition(partitions)
	try:
		if append is 0:
			rddData.write.mode("overwrite").format("com.databricks.spark.csv").option("header","true").option("delimiter","|").option("quote","\\").option("inferSchema","true").option("codec","bzip2").option("treatEmptyValuesAsNulls","true").save(writingPath)	
		else:
			rddData.write.mode("overwrite").format("com.databricks.spark.csv").option("quote","\\").option("inferSchema","true").option("header","true").option("delimiter","|").option("codec","bzip2").option("treatEmptyValuesAsNulls","true").save(writingPath)	

	except (OSError, IOError) as err:
		print("final hive search is empty")
		print ("N history exist for this serach must be new adds")
		return None
	return "great"
def transferSnapshots1(sqlc,fileName,spark):
	totalData = readrddFromcsvfile(fileName,sqlc)
	return totalData

def transferSnapshots(sqlc,filesList,spark):
	totalData = sqlc.createDataFrame(spark.emptyRDD(),StructType([]))
	for each in filesList:
		rdd = readrddFromcsvfile(each,sqlc)
		#if rdd is not None:
		#	print rdd.count()
		try:
			if totalData.rdd.isEmpty() is True :
				if rdd is not None:
					totalData = rdd
			else:
				#print "File name :" + each
				if rdd is not None:
					s1 = rdd.columns
					s2 = totalData.columns
					if len(s1) ==len(s2):
						totalData = totalData.unionAll(rdd)
					else:
						print("There is a column mismatch ignoring rdd")
		except:
			print "ingnoring the issue really dont know the reason of exception"
	return totalData

def transferSnapshotsmulti(sqlc,filesList):
	rdd = readrddFromcsvfiles(filesList,sqlc)
	if rdd is not None:
		return rdd
	return None
def testVersion(spark):
	hc, sqlc = hiveInit(sc_ = spark)
	groupedDict = globalMain(sc_ = spark)
	listfiles = groupedDict['20184913']
	for each in listfiles:
		print each
	print "\n"
	tdata = transferSnapshots(sqlc,listfiles,spark)
	tdata.show()
	return
def mainOps(spark = None,baseSource = 'Not_ODS',todaysDate = 0):
	
	quiet_logs(sc_ = spark)
	#print("Start reading text data")
	hc, sqlc = hiveInit(sc_ = spark)
	groupedDict = globalMain(sc_= spark)
	print("start reading dictionary base")
	dictionaryBase = None
	if baseSource is 'ODS':
		final_list = groupedDict['20184521'] + groupedDict['20184522'] + groupedDict['20184523']
		print len(final_list)
		dictionaryBase = transferSnapshots(sqlc,final_list,spark )#'/npd/s_test2/dictionaryBase/')
		print "size of base :"  + str(dictionaryBase.count())
		print("start writing down dictionary base")
		deletePath('/npd/s_test2/dictionaryBase/',sc_ = spark)
		writeDown(dictionaryBase,'/npd/s_test2/dictionaryBase/')
		print ("end of writing down")
	else:
		dictionaryBase = transferSnapshotsmulti(sqlc,'/npd/s_test2/dictionaryBase/')
		writeDown(dictionaryBase,'/npd/s_test2/dictionaryBase1/')

	todayKey = None
	if todaysDate is 1 :
		todayKey = todaysKey()
			
	deletePath('/npd/s_test2/snapshotFilestemp/',sc_= spark)
	snapshotIndex = 1
	for each in groupedDict.iteritems():
		#writeDown(dictionaryBase,'/npd/s_test2/dictionaryBase/')
		if str(each[0]).find('201845') is not -1:
			print "Base files written on that day"
			continue
		if todaysDate is 1 and each[0] != todayKey:
			print "only need to process todays key"
			continue
		print "snapshot id: " + str(each[0])
		listFiles = listOffiles()
		print "start reading " + str(snapshotIndex) + " snapshot"
		snapshotRdd = transferSnapshots(sqlc,each[1],spark)
		print "start writing " + str(snapshotIndex) + " snapshot"
		writeDown(snapshotRdd,'/npd/s_test2/snapshotFilestemp/')
		dirIndex = 0
		dataSource = ['/npd/ODS/ODS_BZ2/ODS_POSOUTLETITEMS/']
		writingPath = ['/npd/s_test2/snapshotFilesfilter/','/npd/ODS/ODS_BZ2/UNIQUE_ODS_POSOUTLETITEMS/']	
		for file_ in listFiles:
			print "executing file " + str(file_)
			rddData = executeScripts(file_,hc, sqlc,spark)
			amount = rddData.count()
			print "number of data returned: " + str(amount)
			rddData.show()
			if rddData.rdd.isEmpty() == False and file_.find('removeDups') != -1 :
				deletePath("/npd/s_test2/snapshots/withoutdups/",sc_ = spark)
				writeDown(rddData,"/npd/s_test2/snapshots/withoutdups/")
			if rddData.rdd.isEmpty() == False and file_.find('performjoin') != -1:
				print "start filtering dictionary base"
				dictionaryBase = dofiltering(dictionaryBase, rddData)
				#writeDown(rddData,writingPath[dirIndex])
				#dirIndex += 1
				print "Deleting old dictionary base"
				boolean = deletePath('/npd/s_test2/dictionaryBase1/',sc_=spark)	
				dictionaryBase = dictionaryBase.unionAll(rddData)
				print "start writing new dictionary base"
				writeDown(dictionaryBase,'/npd/s_test2/dictionaryBase1/')
				dirIndex += 1
		if dirIndex is 0:
			writeDown(snapshotRdd,'/npd/s_test2/dictionaryBase1/',append = 1)

		print ("deleting snapshot files from temp space")
		deletePath('/npd/s_test2/snapshotFilestemp/',sc_= spark)	
		print ("End of iteration " + str(snapshotIndex))
		snapshotIndex += 1

	print("end of processing")

def init():
	#spark = SparkSession.builder.appName('attribCounter').config("spark.dynamicAllocation.enabled",True).getOrCreate()
 	conf = SparkConf().setAppName("dataOverlapdetector")
    	conf.set("spark.dynamicAllocation.enabled",True)
    	conf.set("spark.yarn.executr.memoryOverhead",8192)
    	conf.set("spark.shuffle.service.enabled",True)
   	conf.set("yarn.nodemanager.vmem-check-enabled",False)
    	conf.set("spark.local.dir","/hdpdata/tmp")
    	conf.set("yarn.nodemanager.vmem-pmem-ratio",6)
    	conf.set("spark.eventLog.enabled",True)
    	#conf.set("spark.driver.memory",128)
    	conf.set("spark.eventLog.dir","/hdpdata/logs")
	conf.set("PYSPARK_DRIVER_PYTHON",False)
	conf.set("yarn.log-aggregation-enable",True)
	conf.set("spark.scheduler.mode","FAIR")
    	sc = SparkContext(conf = conf)
    	sc.setLogLevel("ERROR")
	return sc
#sc = 	init()
#mainOps(spark = sc)
#testVersion(spark= sc)
#sc.stop()
