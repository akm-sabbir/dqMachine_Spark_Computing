from processingUnit import transferSnapshotsmulti
#from processingUnit import init
from processingUnit import dofiltering
from processingUnit import writeDown
from processingUnit import transferSnapshots
from processingUnit import readrddFromcsvfile
from processingUnit import readrddFromcsvfiles
from processingUnit import executeScripts
from processingUnit import hiveExecutecommands
from processingUnit import readFiles
#from updatePositems import startReadingfromhive
from processingUnit import quiet_logs
from processingUnit import  hiveInit
from processingUnit import readFiles 
from hdfsFileDiscovery import globalMain
from hdfsFileDiscovery import deletePath
from hdfsFileDiscovery import initSystem
from pyspark.sql.types import *
from pyspark.sql.functions import avg, udf, col
from pyspark.sql.types import StringType
from processingUnit import transferSnapshots1
from processingUnit import listOffiles
from processingUnit import printwithstats
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from collections import deque
from hdfsFileDiscovery import todaysKey
from pyspark.sql import Row
from pyspark.sql.functions import avg, udf, col
from operator import add
from runningHadoopcmd import run_cmd
from map_generator import generateUPCmap
from map_generator import generateSKUmap
#from readAndload import isSnapshotfiles
from map_generator import generateMODELmap
import sys
import os
import MySQLdb
import pyspark.sql.functions as psf
from collections import defaultdict
from hdfsFileDiscovery import rangeOfdate
from spark_logging import Unique
import math
import scipy
import scipy.stats as ss
from hivewritebackModule import hivewriteback  
from pyspark.sql.functions import to_timestamp
from dynamicRepartition import dynamicRepartition
from find_new_itemid import get_partition_number

def startReadingfromhdfs(listOffiles = None,sqlc = None,spark = None,multi = 0):
	data = sqlc.createDataFrame(spark.emptyRDD(),StructType([]))
	if multi is 1:
		data = transferSnapshotsmulti(sqlc,listOffiles)
	elif multi is 2:
		data = transferSnapshots1(sqlc,listOffiles,spark)
	else:
		
		for each in listOffiles:
			print "File name: " + each
			tempData = transferSnapshots1(sqlc,each,spark)
			try:
				if data.rdd.isEmpty() is True :
					if tempData is not None:
						data = tempData
				else:
					print "File name :" + each
					if tempData is not None:
						s1 = tempData.columns
						s2 = data.columns
						if len(s1) ==len(s2):
							data = data.unionAll(tempData)
						else:
							print("There is a column mismatch ignoring rdd")
			except:	
				print "ingnoring the issue really dont know the reason of it"
	
	return  data

def isSnapshotfiles(dates, listOfdates):
	for each in listOfdates:
		if each == dates:
			return True
	return False

def startOverlapdetector(rddData, l1, sqlc, hc, spark):
	listFiles = listOffiles()
	listFiles = l1 + listFiles
	uniPoiid = None
	if hc is not None:
		print "Initiate overlap detection"
		#print "hive context is not null start executing"
		#print "executing file " + str(file_)
		if rddData.rdd.isEmpty() == False  :
				#deletePath("/npd/s_test2/snapshots/withoutdups/",sc_ = spark)
			#print "Wrting down unique rdd data"
			uniPoiid = rddData.groupBy("poi_id").agg(psf.max("updated").alias("updated_")).distinct()
			uniPoiid = uniPoiid.selectExpr(["poi_id as poi_id_", "updated_ as updated_"])
			uniPoiid = uniPoiid.withColumn("poi_id_",uniPoiid["poi_id_"].cast("long"))
			uniPoiid.persist()
			#rddData.show()
			#writeDown(rddData,"/npd/s_test2/snapshots/withoutdups/")
		if rddData.rdd.isEmpty() == False :
			rddData = rddData.withColumn("poi_id",rddData["poi_id"].cast("long"))
			#timeFmt="yyyy-mm-dd'T'HH:mm:ss"
			timeFmt="yyyy-mm-dd HH:mm:ss"
			timeDiff = psf.abs(psf.unix_timestamp("rddData.updated",format = timeFmt) - psf.unix_timestamp("unirdd.updated_",format = timeFmt))
			rddData.persist()
			rddData = rddData.alias("rddData").join(uniPoiid.alias("unirdd"),psf.col("rddData.poi_id") == psf.col("unirdd.poi_id_"),'inner').where(timeDiff == 0)
			droplist = ['poi_id_','updated_']
			rddData = rddData.select([column for column in rddData.columns if column not in droplist])
			#print "Returned Rdd size is :" + str(rddData.count())
			print "End of overlap detection"
			return rddData
	else:
		snapshotRdd.registerTempTable("snapshotRdd")
		print "Getting distinct partition names"
		uniquepoiidRdd = sqlc.sql("select poi_id, max(distinct updated)  as updated from snapshotRdd group by poi_id") 
		base.withColumn("updated",base["updated"].cast("string"))
		cond = [snapshotRdd.poiid== uniquepoiidRdd.poiid, abs(unix_timestamp(snapshotRdd.withColumn("updated",snapshotRdd["updated"].cast("string"))) - unix_timestamp(uniquepoiidRdd.withColumn("updated", uniquepoiidRdd["updated"].cast("string")))) == 0]
		snapshotRdd = snapshotRdd.alias('snapshotRdd').join(uniquepoiidRdd,cond,'inner')
		sqlc.dropTempTable("snapshotRdd")
		if snapshotRdd.rdd.isEmpty() is False:
			print "size of snapshot Rdd: " + str(snapshotRdd.count())
			return snapshotRdd
	return sqlc.createDataFrame(spark.emptyRDD(),StructType([]))

def startFilteringfrombase(base,snapshot):
	base = base.filter(base.poi_id != snapshot.poi_id)
	return base
def mainOps(spark = None,baseSource = 'Not_ODS',snapshotsNeedread = False,todaysDate = 0):
	
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
	if baseSource is 'ODS_BASE':
		dictionaryBase = startReadingfromhdfs(sqlc = sqlc,listOffiles = '/npd/s_test2/dictionaryBase/',multi = 1,spark=spark)
		deletePath('/npd/s_test2/dictionaryBase1/',sc_= spark)
		writeDown(dictionaryBase,'/npd/s_test2/dictionaryBase1/')

	if snapshotsNeedread is True:		
		deletePath('/npd/s_test2/snapshotFilestemp/',sc_= spark)
		snapshotIndex = 1
		totalsnapShots = sqlc.createDataFrame(spark.emptyRDD(),StructType([]))
		fileList = []
		for each in groupedDict.iteritems():
			if str(each[0]).find('201845') is not -1:
				print "Base files written on that day"
				continue
			print "snapshot id: " + str(each[0])
			#print "start reading " + str(snapshotIndex) + " snapshot"
			fileList = fileList + each[1]
		print fileList
		snapshotRdd = startReadingfromhdfs(sqlc = sqlc,listOffiles = fileList,spark = spark)
		print "start writing snapshot files"
		writeDown(snapshotRdd,'/npd/s_test2/snapshotFilestemp/')
	nondupSnapshotrdd = startOverlapdetector(snapshotRdd,['./createExternalTable.sql'],sqlc,hc,spark)
	#nondupBaserdd = startOverlapdetector(['./createExttabledictbase.sql'],sqlc,hc,spark)
	#colNames = columnRenaming(listNames)
	#snapshotBase = snapshotBase.selectExpr(colNames)
	array= nondupSnapshotrdd.select(['poi_id'])
	array = array.rdd.map(lambda x : x.poi_id).collect()
	#array = [lit(poi_id).alias("poi_id").cast("long") for poi_id  in array] 
	base = nondupBaserdd.where(~col("poi_id").isin(array)) 
	#base = startFilteringfrombase(nondupBaserdd,nondupSnapshotrdd)
	base = base.unionAll(nondupSnapshotrdd)
	base = base.withColumn("updated",base["updated"].cast("string"))
	base = base.withColumn("added",base["added"].cast("string"))
	print "dictionary base size: " + str(base.count())
	deletePath('/npd/s_test2/uniqueBasedictionary',sc_=spark)
	writeDown(base,'/npd/s_test2/uniqueBasedictionary')
	upc_map = generateUPCmap(rdddata=base)
	sku_map = generateSKUmap(rdddata=base)
	model_map = generateMODELmap(rdddata=base)
	return 
def getColumnsname(with1 = 0):
	if with1 is 0:
		listNames = ["poi_id" ,"business_id" ,"posoutlet" ,"outletdivision"  ,"outletdepartment" ,"outletsubdepartment" ,"outletclass" ,"outletsubclass" ,"outletbrand" ,"outletitemnumber","outletdescription" ,"outletbrandmatch" ,"outletitemnumbermatch" ,"outletdescriptionmatch" ,"sku" ,"manufacturercodetype" ,"manufacturercode" ,"zzzppmonthfrom" ,"zzzppmonthto" , "zzzppmonthlastused" ,"itemid" ,"itemtype" ,"price" ,"manufacturercodestatus" ,"loadid" ,"status" ,"added" ,"updated","ppweekfrom" ,"ppweekto" ,"ppweeklastused" ,"matched_country_code" ,"previous_poiid" ,"include_data_ppmonthfrom" ,"include_data_ppweekfrom" ,"manufacturercodematch" ,"skumatch" , "unitofmeasure" ,"packsize" ,"manufacturername" ,"manufacturernamematch" ,"privatelabel" ,"outletdescriptionsupplement" ,"total_confidence_score" , "parent_poiid" ,"parent_poiid_status"]

	else:
		listNames = ["poi_id1" ,"business_id1" ,"posoutlet1" ,"outletdivision1"  ,"outletdepartment1" ,"outletsubdepartment1" ,"outletclass1" ,"outletsubclass1" ,"outletbrand1" ,"outletitemnumber1","outletdescription1" ,"outletbrandmatch1" ,"outletitemnumbermatch1" ,"outletdescriptionmatch1" ,"sku1" ,"manufacturercodetype1" ,"manufacturercode1" ,"zzzppmonthfrom1" ,"zzzppmonthto1" , "zzzppmonthlastused1" ,"itemid1" ,"itemtype1" ,"price1" ,"manufacturercodestatus1" ,"loadid1" ,"status1" ,"added1" ,"updated1","ppweekfrom1" ,"ppweekto1" ,"ppweeklastused1" ,"matched_country_code1" ,"previous_poiid1" ,"include_data_ppmonthfrom1" ,"include_data_ppweekfrom1" ,"manufacturercodematch1" ,"skumatch1" , "unitofmeasure1" ,"packsize1" ,"manufacturername1" ,"manufacturernamematch1" ,"privatelabel1" ,"outletdescriptionsupplement1" ,"total_confidence_score1" , "parent_poiid1" ,"parent_poiid_status1"]

	return listNames
def columnRenaming(listNames):
    cols = []
    for ind, each in enumerate(listNames):
    	cols.append('_c' + str(ind) + ' as '  + listNames[ind])
    return cols
def startFilteringstep(spark):
	hc, sqlc = hiveInit(sc_ = spark)
	dictionaryBase = startReadingfromhdfs(sqlc = sqlc,listOffiles = '/npd/s_test2/uniqueBasedictionary', multi = 1, spark = spark)
	dictionaryBase.show()
        listNames = ["poi_id" ,"business_id" ,"posoutlet" ,"outletdivision"  ,"outletdepartment" ,"outletsubdepartment" ,"outletclass" ,"outletsubclass" ,"outletbrand" ,"outletitemnumber","outletdescription" ,"outletbrandmatch" ,"outletitemnumbermatch" ,"outletdescriptionmatch" ,"sku" ,"manufacturercodetype" ,"manufacturercode" ,"zzzppmonthfrom" ,"zzzppmonthto" , "zzzppmonthlastused" ,"itemid" ,"itemtype" ,"price" ,"manufacturercodestatus" ,"loadid" ,"status" ,"added" ,"updated","ppweekfrom" ,"ppweekto" ,"ppweeklastused" ,"matched_country_code" ,"previous_poiid" ,"include_data_ppmonthfrom" ,"include_data_ppweekfrom" ,"manufacturercodematch" ,"skumatch" , "unitofmeasure" ,"packsize" ,"manufacturername" ,"manufacturernamematch" ,"privatelabel" ,"outletdescriptionsupplement" ,"total_confidence_score" , "parent_poiid" ,"parent_poiid_status"]
	colNames = columnRenaming(listNames)
	dictionaryBase = dictionaryBase.selectExpr(colNames)
	dictionaryBase.show()
	snapshotBase = startReadingfromhdfs(sqlc = sqlc,listOffiles = '/npd/s_test2/uniqueSnapshotFilestemp', multi = 1, spark = spark)
	snapshotBase.show()
	listNames1 = ["poi_id1" ,"business_id1" ,"posoutlet1" ,"outletdivision1"  ,"outletdepartment1" ,"outletsubdepartment1" ,"outletclass1" ,"outletsubclass1" ,"outletbrand1" ,"outletitemnumber1","outletdescription1" ,"outletbrandmatch1" ,"outletitemnumbermatch1" ,"outletdescriptionmatch1" ,"sku1" ,"manufacturercodetype1" ,"manufacturercode1" ,"zzzppmonthfrom1" ,"zzzppmonthto1" , "zzzppmonthlastused1" ,"itemid1" ,"itemtype1" ,"price1" ,"manufacturercodestatus1" ,"loadid1" ,"status1" ,"added1" ,"updated1","ppweekfrom1" ,"ppweekto1" ,"ppweeklastused1" ,"matched_country_code1" ,"previous_poiid1" ,"include_data_ppmonthfrom1" ,"include_data_ppweekfrom1" ,"manufacturercodematch1" ,"skumatch1" , "unitofmeasure1" ,"packsize1" ,"manufacturername1" ,"manufacturernamematch1" ,"privatelabel1" ,"outletdescriptionsupplement1" ,"total_confidence_score1" , "parent_poiid1" ,"parent_poiid_status1"]
	#ilistNames1 = ["poi_id1","business_id1","posoutlet1","outletdivision1", "outletdepartment1","outletclass1","outletbrand1","outletitemnumber1" ,"outletdescription1","outletbrandmatch1","manufacturercode1", "sku1", "itemid1", "itemtype1" , "price1" ,"manufacturercodestatus1" ,"loadid1", "status1", "added1", "updated1" , "matched_country_code1", "previous_poiid1", "parent_poiid1" ," parent_poiid_status1"]
	colNames = columnRenaming(listNames)
	snapshotBase = snapshotBase.selectExpr(colNames)
	array= snapshotBase.select(['poi_id'])
	array = array.rdd.map(lambda x : x.poi_id).collect()
	#array = [lit(poi_id).alias("poi_id").cast("long") for poi_id  in array] 
	base = dictionaryBase.where(~col("poi_id").isin(array)) 
	#startFilteringfrombase(dictionaryBase,snapshotBase)
	base = base.unionAll(snapshotBase)
	writeDown(base ,'/npd/s_test2/finalResults/')
	return
def getLastfilenumber(filenames = None,ft = 0 , rw = 0,file_num_ = None, debug = False, rev = True):
	file_number  = None
	parentPath = 'src/main/python/dictionary/configuration/lastfilenumber' if ft is 0 else 'src/main/python/dictionary/configuration/lastfilenumberOds'
	needToread = []
	if rw is 0:
	   with open( parentPath ) as filereader:
	      file_number = filereader.readline()
	   filenames = sorted(filenames,reverse = rev)
	   #needToread = []
	   if debug is True:
		for each in filenames:
			for sub_each in each[1]:
				print "day is: " + str(each[0]) + " filename is: " + str(sub_each)
           	print( filenames)
	   for each in filenames:
	      if (each.split('/')[-1] > file_number):
	         needToread.append(each)
	   file_number = filenames[-1].split("/")[-1]
	   last_file_number = filenames[0].split("/")[-1]
        else:
	   with open( parentPath ,'w+') as datawriter:
              if file_num_ is not None:
		 print "last file name: " + file_num_
	         datawriter.write(str(file_num_))
	   return None, None
		
	return needToread, last_file_number, file_number
def uploadItemidMapper():
	run_cmd(['hdfs', 'dfs', '-rm', "hdfs:////npd/s_test2/itemidMapper/itemIdWithPartition08.txt"])
	run_cmd(['hdfs', 'dfs', '-rm', "hdfs:////npd/s_test2/partitionList/partition_list.txt"])
	s_r,s_o,s_e = run_cmd(['hdfs', 'dfs', '-copyFromLocal', 'src/main/python/dictionary/fileSource/partition_list.txt', "hdfs:////npd/s_test2/partitionList/"])

	s_r,s_o,s_e = run_cmd(['hdfs', 'dfs', '-copyFromLocal', 'src/main/python/dictionary/maps/itemIdWithPartition08.txt', "hdfs:////npd/s_test2/itemidMapper/"])
	print str(s_r) +str(' and ')+ str(s_o) +' and '+ str(s_e)
	return
def writebackTohive(dictDataframe,append = 1):
	writeMode = "overwrite" if append is 0 else "append"
	dictDataframe.repartition("partitioner","business_id").write.mode(writeMode).partitionBy("business_id","partitioner").format("orc").saveAsTable("dqdictionaryhivedb.uniqueodsposoutlet2_int")
	return
def addPartitionColumn(rdd):
	if rdd.columns.count("partitioner") is 1 :
		rdd = rdd.drop("partitioner")
	#rdd = rdd.withColumn("partitioner","partitioner_" + str(int(math.fmod(rdd.poi_id, 800))))
	rdd = rdd.withColumn("poi_id",rdd["poi_id"].cast("long"))
	tempRdd = rdd.select("poi_id").rdd.map(lambda x : Row(poi_id_ = x.poi_id ,partitioner= "partitioner_" + str((int(math.fmod(hash(x.poi_id),400)) + 600)))).toDF()
	tempRdd = tempRdd.withColumn("poi_id_",tempRdd["poi_id_"].cast("long"))
	rdd = rdd.alias("basetemp").join(tempRdd.alias("tempRdd"), (psf.col("basetemp.poi_id") == psf.col("tempRdd.poi_id_")),"inner")
	rdd = rdd.drop("poi_id_")
	return rdd
def redistributePartition(rdd):
	rdd = rdd.where(rdd.partitioner == 'partitioner_305')
	rdd = rdd.drop('partitioner')
	rdd = addPartitionColumn(rdd)
	return rdd
def createBroadcast(rdd, spark):
	return  spark.broadcast(rdd)

def filterBasefiles(listOffiles, lastfileNumber = 'part-000000.bz2'):
	newList = []
	try:
		for each in listOffiles:
		
			if(each.split("/")[-1] >= lastfileNumber):
				continue
			else:
				newList.append(each)
	except ValueError as ve:
		print "Gettign a value error " + str(ve) 
	return newList

def updatePosoutletwithsnapshots(spark = None, ranges = 0, repartBase = 1, lastFilenumber = None, configOb = None, filtering = 1, table_index = 2 ):
	#if len(sys.argv) == 2:
        #	print("Usage: snapshotdetector number of days" + str(sys.argv[1]))
	#	ranges = int(sys.argv[1])
	print "Start working on updating the posoutlet table"
	quiet_logs(sc_ = spark)
	hc, sqlc = hiveInit(sc_ = spark)
	groupedDict = globalMain(pathName = configOb.input_path[2], sc_= spark)
	bis_list = configOb.businessSets
	dp = dynamicRepartition.factoryFunc(file_name = 'src/main/python/dictionary/configuration/resource_config')
	print("start reading snapshots for posoutlet")
	print "retrieve the table name"
	table_name = configOb.hivedbOb.get_dbName(index = 0) + "." + configOb.hivedbOb.get_tabNames(dbName_ = configOb.hivedbOb.get_dbName(index = 0), index = table_index)
	date_list, hr_ = rangeOfdate(ranges) if ranges is not 0 else todaysKey(delta = configOb.delta)
	totalsnapShots = sqlc.createDataFrame(spark.emptyRDD(),StructType([]))
	fileList = []
	fileList_for_base = []
	print "dateList is :" + str(date_list)
	for each in groupedDict.iteritems():
		if isSnapshotfiles(each[0].split()[0] ,date_list) is False:
			fileList_for_base = fileList_for_base + each[1]
			continue
		fileList = fileList + each[1]
	printwithstats(fileList)	
	if len(fileList )is 0:
		print "snapshot File list is emtpy for odsposoutlet table so..."
		print "Its empty exiting"
		exit(0)
	for each in fileList:
		print "Snapshot files: " + str(each)
	print ("Get the filtered files to read")
	needToread,last_file_num, fn = getLastfilenumber(fileList)
	print "Last file number for snapshot: " + str(last_file_num)
	if len(needToread) is 0 :
		print "snapshot files are all updated nothing to work on"
		print "application is exiting"
		sys.exit(0)
	fileList_for_base = filterBasefiles(fileList_for_base,lastfileNumber = fn)
	_, last_file_num_base, _ = getLastfilenumber(fileList_for_base, rev = True)
	print "Last file number for base: " + str(last_file_num_base)
	print ("Start reading snapshot files")
	stringedList = ",".join(needToread)
	snapshotRdd = startReadingfromhdfs(sqlc = sqlc, listOffiles = needToread, spark = spark, multi = 2)
	print "End of reading snapshot files"
	#print "start repartitioning the snapshsot file"
	#snapshotRdd = snapshotRdd.repartition(400)
	#print "End of repartition of snapshot dataframe"
	bis_list = [str(each) for each in bis_list] if bis_list is not None else None
	query = "select " +",".join(getColumnsname()) + ", partitioner  from dqdictionaryhivedb.uniqueodsposoutlet2_int" 
	query = query + "where business_id in (" +','.join(bis_list) + ")" if bis_list is not None else query
	baseDict = startReadingfromhdfs(sqlc = sqlc,listOffiles = fileList_for_base , multi = 2, spark = spark) if configOb.read_hive_odspos is 0 else startReadingfromhive(query = query, hc = hc, sqlc=sqlc,spark = spark)
	#baseDict = baseDict.coalesce(10000) if baseDict.rdd.getNumPartitions() > 10000 else baseDict
	#baseDict = baseDict.repartition(2500) if repartBase is 1 else baseDict
	#snapshotRdd = snapshotRdd.repartition(baseDict.rdd.getNumPartitions())
	listofC = getColumnsname()
	cols = columnRenaming(listofC)
	snapshotRdd = snapshotRdd.selectExpr(cols)
	bis_list = [ int(each) for each in bis_list] if bis_list is not None else None
	snapshotRdd = snapshotRdd.where(snapshotRdd.business_id.isin(bis_list)) if filtering is 1 else snapshotRdd
	snapshotWithpartition, itemidRdd = addingPartitionersnapshot(snapshotRdd , spark, sqlc)
	baseDict = baseDict.selectExpr(cols) if configOb.read_hive_odspos is 0 else baseDict
	baseDict = baseDict.where(baseDict.business_id.isin(set(bis_list))) if filtering is 1 else  baseDict
	print "Start repartitioning the base files"
	dictCount = baseDict.count()
	dp.set_data_size(dictCount)
	resizer = dp.dynamicPartitionresizer()
	actualSize = get_partition_number(baseDict,resizer,configOb.partition_size_change)
	baseDict = baseDict.repartition(actualSize) if actualSize is not 0 else baseDict
	print "End of repartitioning the base"
	print "Start of repartitioning the snapshots"
	snapCount = snapshotRdd.count()
	dp.set_data_size(snapCount)
	resizer = dp.dynamicPartitionresizer()
	actualSize = get_partition_number(snapshotRdd, resizer,configOb.partition_size_change )
	snapshotRdd = snapshotRdd.repartition(actualSize) if actualSize is not 0 else snapshotRdd
	print "End of repartitioing the snapshot files"
	#baseDict = baseDict.repartition(actualSize) if actualSize is not 0 else baseDict
	print "Start updating the posoutlet table"
	updateOdsposoutlet(snapshotRdd, baseDict = baseDict, itemidRdd = itemidRdd, process_dict = 1, spark = spark , ranges = ranges, readHdfs = configOb.read_hive_odspos, repartBase = repartBase, appendMode = 0, addpartitionCol = 1, process_zero = 0, fileList = fileList, rddwithPartition = snapshotWithpartition, lastFilenumber = last_file_num, table_name = table_name, hdfs_output = configOb.input_path[0],configOb = configOb )
	return
def stringTotimestamp(rdd, cols = [], formats = 'yyyy_MM_dd', types = 'int'):
        #to_timestamp('dt_col','yyyy_MM_dd_hh_mm_ss')
	for each in cols:
		rdd = rdd.withColumn(each, rdd[each].cast(types))
	return rdd
def updateOdsposoutlet(snapshotRdd, baseDict = None, itemidRdd = None, process_dict = 0, spark = None , ranges = 2, readHdfs = 1, repartBase = 0, appendMode = 0, addpartitionCol = 0, process_zero = 0, listOffiles = None, fileList = None, rddwithPartition = None, lastFilenumber = None ,configOb = None, table_name = None, hdfs_output = '/npd/s_test2/uniqueBasedictionary/', debug = 0, writeTohdfs = 0):
	hc, sqlc = hiveInit(sc_ = spark)
	if configOb is None :
		print "configuration object can not be None"
		print "system exiting"
		sys.exit(0)
	if len(fileList ) is 0:
		print "Its empty exiting"
		exit(0)
	_, last_file_num, _ = getLastfilenumber(fileList)
	table_name = configOb.hivedbOb.get_dbName(index = 0) + "." + configOb.hivedbOb.get_tabNames(dbName_ = configOb.hivedbOb.get_dbName(index = 0), index = 2 ) if table_name is None else table_name

	if baseDict is None:
		print "base has to be updated can not be None"
		print "End of the operation no update operation for base dictionary"
		sys.exit(0)
	print ("add partitions to snapshot data")
	snapshotRdd , itemidRdd, tracker = automation(rdd = snapshotRdd, joinedRdd = rddwithPartition, itemidRdd = itemidRdd, spark = spark, hc = hc, sqlc = sqlc)
	print ("Start detecting overlap data and return uniques")
	nondupSnapshotrdd = startOverlapdetector(snapshotRdd, ['src/main/python/dictionary/fileSource/hivecreateScripts/createExternalTable.sql'], sqlc, hc, spark)
	baseDict = startOverlapdetector(baseDict, ['src/main/python/dictionary/fileSource/hivecreateScripts/createExternalTable.sql'], sqlc, hc, spark) if process_dict is 1 else baseDict
	print "End of detecting the overlap data and return the uniques"
	print "start seperating the zero itemids"	
	zeroRdd = nondupSnapshotrdd.where(nondupSnapshotrdd.itemid == 0) if process_zero is 1 else sqlc.createDataFrame(spark.emptyRDD(), StructType([]))
	nondupSnapshotrdd = nondupSnapshotrdd.filter(nondupSnapshotrdd.itemid != 0) if zeroRdd.rdd.isEmpty() is False else nondupSnapshotrdd
	zerobaseRdd = baseDict.where(baseDict.itemid == 0) if process_zero is 1 else sqlc.createDataFrame(spark.emptyRDD(), StructType([]))
	baseDict = baseDict.filter(baseDict.itemid != 0) if zerobaseRdd.rdd.isEmpty() is False else baseDict
	#print "size of  base dictionary after filtering 0 itemid: " + str(baseDict.count())
	zeroRdd = zeroRdd.withColumn("poi_id",zeroRdd["poi_id"].cast("long")) if zeroRdd.rdd.isEmpty() is False else zeroRdd
	zeroRdd = addPartitionColumn(zeroRdd) if zeroRdd.rdd.isEmpty() is False else zeroRdd
	zeroRdd = zeroRdd.withColumn("partitioner",zeroRdd["partitioner"].cast("string")) if zeroRdd is False else zeroRdd
	#print "size of non zero snapshot itemid: " + str(nondupSnapshotrdd.count())
	print "start seperating the zero itemid for base dictionary"
	zerobaseRdd = zerobaseRdd.repartition(baseDict.rdd.getNumPartitions()) if zerobaseRdd.rdd.isEmpty() is False else zerobaseRdd
	zerobaseRdd = zerobaseRdd.withColumn("poi_id",zerobaseRdd["poi_id"].cast("long")) if zerobaseRdd.rdd.isEmpty() is False else zerobaseRdd
	zerobaseRdd = addPartitionColumn(zerobaseRdd) if zerobaseRdd.rdd.isEmpty() is False else zerobaseRdd
	zerobaseRdd = zerobaseRdd.withColumn("partitioner", zerobaseRdd["partitioner"].cast("string")) if zerobaseRdd.rdd.isEmpty() is False else zerobaseRdd
	#print "size of non zero itemid: " + str(zerobaseRdd.count())
	if nondupSnapshotrdd.rdd.isEmpty() is True:
		print "snapshot rdd is empty"	
		print "if Non dup snapshots are empty we can avoid overwriting database and hdfs"
		print "system is exiting"
		sys.exit(0)
	print "End of seperating the zero itemids for base dictionary"
	#print ("Find the partition for each itemid")
	final_rdd = nondupSnapshotrdd
	#final_rdd, itemidRdd, tracker = automation(rdd = nondupSnapshotrdd,joinedRdd = rddwithPartition, itemidRdd = itemidRdd,spark = spark, hc = hc, sqlc = sqlc)
	final_rdd = final_rdd.unionAll(zeroRdd) if zeroRdd.rdd.isEmpty() is False else final_rdd
	print ("type cast the updated and added date column")
	final_rdd = final_rdd.withColumn("updated",final_rdd["updated"].cast("string"))
	final_rdd = final_rdd.withColumn("added",final_rdd["added"].cast("string"))
	print ("read the base dictionary" )
	########### Reading unique base dictionary using spark csv reader ########################
        ################# reading unique base dictionary using hive external table #######################################
	print ("perform left anti join on poi_id to retrieve unique poi_id based records")
	#print ("final_rdd size before left anti join " + str(final_rdd.count()))
	#condition_list = [psf.col("basetemp.itemid") == psf.col("finalrddtemp.itemid"),psf.col("basetemp.poi_id") == psf.col("finalrddtemp.poi_id")]
	baseDict = baseDict.withColumn("itemid",baseDict["itemid"].cast("long"))
	baseDict = baseDict.withColumn("poi_id",baseDict["poi_id"].cast("long"))
	final_rdd = final_rdd.withColumn("poi_id",final_rdd["poi_id"].cast("long"))
	itemidRdd = itemidRdd.withColumn("vitemid",itemidRdd["vitemid"].cast("long"))
	condition_list = [psf.col("basetemp.itemid") == psf.col("finalrddtemp.itemid"),psf.col("basetemp.poi_id") == psf.col("finalrddtemp.poi_id")]
	if readHdfs  is 1:
		baseDict.persist()
		itemidRdd.persist()
	baseDict = baseDict.alias('basedict').join(itemidRdd.alias('itemidrdd'),(psf.col("basedict.itemid") == psf.col("itemidrdd.vitemid")),'inner') if addpartitionCol is 1 else baseDict
	#print "size of the base dictionary after adding the partitioner column: " + str(baseDict.count())
	baseDict = baseDict.drop("vitemid") if addpartitionCol is 1 else baseDict
	#baseDict = baseDict.select([column for column in baseDict.columns if column not in droplist])
	#final_rdd = createBroadcast(final_rdd, spark)
	baseDict = baseDict.alias("basetemp").join(final_rdd.alias("finalrddtemp"), (psf.col("basetemp.poi_id") == psf.col("finalrddtemp.poi_id")),"leftanti") #if appendMode is 0 else final_rdd.alias("finalrddtemp").join(baseDict.alias("basetemp"), (psf.col("finalrddtemp.poi_id") == psf.col("basetemp.poi_id")),"leftanti")
	baseDict = baseDict.unionAll(final_rdd) if configOb.append_in_hive is 0 else baseDict
	baseDict = baseDict.unionAll(zerobaseRdd) if zerobaseRdd.rdd.isEmpty()  is False else baseDict
	#if debug is True :
	#	print ("final base dict size after left anti join and union of new add all " + str(baseDict.count()))
	#baseDict = baseDict.unionAll(final_rdd)
	print "Repartition the base dictionary data before start writing"
	baseDict = baseDict.repartition(400)
	#print "count of basedictionary data: " + str(baseDict.count())
	#listOfdata =baseDict.groupBy("partitioner").count().select( "partitioner" , psf.col("count").alias("counting")).rdd.map(lambda x:(x.partitioner,x.counting)).collect()
	#for each in listOfdata:
	#	print "partitioner: " + str(each[0]) +" count: " + str(each[1])
	if writeTohdfs is 1:
		print "delete the /npd/s_test2/uniqueBasedictionary path before writing it back"
		deletePath(hdfs_output, sc_ = spark)
		print "writing down the unique snapshots retrieved"
		writeDown(baseDict, hdfs_output)
		print "End of writing down the unique table into hdfs"
	print "Start writing back to hive table"
	#print "get a hive write back object"
	#hivewritingback  = hivewriteback(spark = spark)
	#hivewritingback.insertIntopartitiontable(partitionFiles = listOffiles , dictRdd = baseDict, append = 0)
	#writebackTohive(baseDict,append = 0 if appendMode is 0 else 1 ) 
	#print "End of hive transfer"
	print "Update lastfile read number in configuration file"
	getLastfilenumber(rw = 1, file_num_ = last_file_num, ft = 0)
	print("updating itemIdWithPartition file with new information")
	if len(tracker) is not 0:
		print "start of updating itemidpartition.txt file with new add itemid"
		with open('src/main/python/dictionary/maps/itemIdWithPartition08.txt','a+') as dataWriter:
			for key,value in tracker.iteritems():
				dataWriter.write("{}\n".format(str(1) + '\t' + str(key) + '\t' + value.strip()))
		print "End of itemidpartition file with new add itemid"
	#executeScripts('src/main/python/dictionary/fileSource/hivecreateScripts/createFinaldatatransfer.sql',hc, sqlc,spark)
	print "start type casting for date columns"
	baseDict = stringTotimestamp(baseDict, cols = ['updated','added'], formats = 'yyyy_MM_dd_hh_mm_ss', types = 'timestamp')
	print "end of type casting for date columns"
	#print "Get a hive write back object to write backe to hdfs"
	if configOb.stage['updatehivetable'] is 1 :
		print "Get a hive write back object to write backe to hdfs"
		hivewritingback  = hivewriteback(spark = spark)
		hivewritingback.setTablename(tableName = table_name)
		#if configOb.append_in_hive is 0:
		#hivewritingback.insertIntopartitiontable(partitionFiles = listOffiles , dictRdd = baseDict, append = 0, table_name = table_name)
		#else:
		hivewritingback.insertIntobucketedtable(partitionFiles = listOffiles, dictRdd = baseDict, append = 0 if configOb.append_in_hive is 0 else 1, table_name = table_name, numberOfbus = 4, cols = ["business_id","partitioner"], hc = hc )
	#writebackTohive(baseDict,append = 0 if appendMode is 0 else 1 ) 
		print "End of transfer of data into hive internal table"
		print "Table been updated and written back successfully into hive"
	return "Successful completion of posoutlet table update and written back to hdfs"

def assignPartition(rdd,tempRdd,dq,sqlc):
	tracker = defaultdict(str)
	def local_func(elem):
		if tracker[elem.itemid] is not '':
			return Row(vitemid = elem.itemid,partitioner = tracker[elem.itemid])
		else:
			partition = dq.popleft()
			dq.append(partition)
			tracker[elem.itemid] = partition
			return Row(vitemid = elem.itemid,partitioner = partition)
	#listName.append("partitioner")
	#print("updating itemIdWithPartition file with new information")
	#tempRdd = rdd.select("itemid").distinct()
	tempRdd = tempRdd.rdd.map(lambda x : local_func(x)).toDF()
	#tempRdd.registerTempTable("itemidPar")
	#tempRdd = tempRdd.select("vitemid","partitioner").distinct()#sqlc.sql("select distinct vitemid, partitioner from itemidPar group by vitemid,partitioner")
	#print "temporary distinct rdd size is: "  + str(tempRdd.count()	)
	#rdd = rdd.join(tempRdd, rdd["itemid"] == tempRdd['itemid'],"inner")
	rdd = rdd.alias("rddtemp").join(tempRdd.alias("temp"),(psf.col("rddtemp.itemid") == psf.col("temp.vitemid")),"inner")
	#print "after adding partitioner column rdd size: " + str(rdd.count())
	droplist = ['vitemid']
	#rdd = rdd.select([column for column in rdd.columns if column not in droplist])
	rdd = rdd.drop("vitemid")
	return rdd,tracker

def addingPartitionersnapshot(rdd, spark, sqlc):
	print "upload itemIdWithPartition08.txt file"
	uploadItemidMapper()
	print "Reading itemId meta data"
	itemidRdd = spark.textFile("hdfs:////npd/s_test2/itemidMapper/itemIdWithPartition08.txt", minPartitions = 200)
	print "getting the last two columns"
	itemidRdd = itemidRdd.map(lambda x :Row(vitemid = x.split('\t')[1].strip(),partitioner = x.split('\t')[2])).toDF()
	#rddB = createBroadcast(rdd, spark)
	print "creating a temptable"
	itemidRdd.registerTempTable("itemidPar")
	print "Getting distinct partition names"
	#uniquePart = itemidRdd.select("partitioner").distinct()#sqlc.sql("select distinct partitioner from itemidPar")
	#uniquePart = uniquePart.rdd.map(lambda x : x.partitioner).collect()
	#print uniquePart
	rdd.registerTempTable("Rdd")
	print "Performing the join operation"
	joinedRdd = sqlc.sql("select poi_id ,business_id ,posoutlet ,outletdivision  ,outletdepartment ,outletsubdepartment ,outletclass ,outletsubclass ,outletbrand ,outletitemnumber,outletdescription ,outletbrandmatch ,outletitemnumbermatch ,outletdescriptionmatch ,sku ,manufacturercodetype ,manufacturercode ,zzzppmonthfrom ,zzzppmonthto , zzzppmonthlastused ,itemid ,itemtype ,price ,manufacturercodestatus ,loadid ,status ,added ,updated,ppweekfrom ,ppweekto ,ppweeklastused ,matched_country_code ,previous_poiid ,include_data_ppmonthfrom ,include_data_ppweekfrom ,manufacturercodematch ,skumatch , unitofmeasure ,packsize ,manufacturername ,manufacturernamematch ,privatelabel ,outletdescriptionsupplement ,total_confidence_score , parent_poiid ,parent_poiid_status, dq.partitioner as partitioner from itemidPar as dq inner join Rdd as rdd on dq.vitemid = rdd.itemid")
	sqlc.dropTempTable("Rdd")
	sqlc.dropTempTable("itemidPar")
	return joinedRdd, itemidRdd

def automation(rdd = None, joinedRdd = None ,itemidRdd = None, spark= None,hc = None, sqlc = None ):
	#joinedRdd, itemidRdd = addingPartitionersnapshot(rdd)
	#print "total rdd results: " + str(rdd.count())
	print "perform leftanti join"
	rdd = rdd.alias("rddtemp").join(joinedRdd.alias("joinedrddtemp"),(psf.col("rddtemp.itemid") == psf.col("joinedrddtemp.itemid")),"leftanti")
	print "total rdd results size after filter we need to distribute these items among different partition: " + str(rdd.count())
	uniquePart = itemidRdd.select("partitioner").distinct()#sqlc.sql("select distinct partitioner from itemidPar")
	tempRdd = rdd.select("itemid").distinct() if rdd.rdd.isEmpty() is False else sqlc.createDataFrame(spark.emptyRDD(),StructType([]))
	#tempRddB = createBroadcast(tempRdd,spark)
	#rdd = rdd.repartition(800) if rdd.rdd.isEmpty() is False else rdd
	uniquePart = uniquePart.rdd.map(lambda x : x.partitioner).collect() if rdd.rdd.isEmpty() is False else []
	dq = deque(uniquePart) if len(uniquePart) is not 0 else deque()
	rdd,tracker = assignPartition(rdd, tempRdd, dq, sqlc) if rdd.rdd.isEmpty() is False else (sqlc.createDataFrame(spark.emptyRDD(),StructType([])),defaultdict(str))
	rdd = rdd.unionAll(joinedRdd) if rdd.rdd.isEmpty() is False else joinedRdd
	#print("total count after union of all data: " + str(rdd.count()))
	return rdd,itemidRdd,tracker
def cleanUp():
	quiet_logs(sc_ = spark)
	#print("Start reading text data")
	hc, sqlc = hiveInit(sc_ = spark)
	groupedDict = globalMain(sc_= spark)
	print("start reading dictionary base")
	return
def testFunction(spark):
	hc, sqlc = hiveInit(sc_ = spark)
	#rdd = startReadingfromhdfs(listOffiles = "/npd/s_test2/snapshots/withoutdups/",sqlc = sqlc,spark = spark,multi = 1)
	#rdd.show()
	try:
		run_cmd(['hdfs', 'dfs', '-rm', "hdfs:////npd/s_test2/itemidMapper/itemIdWithPartition06.txt"])
	except:
		print "ignoring the exception"
		pass
	s_r,s_o,s_e = run_cmd(['hdfs', 'dfs', '-copyFromLocal', 'src/main/python/dictionary/maps/itemIdWithPartition06.txt', "hdfs:////npd/s_test2/itemidMapper/"])
	print str(s_r) +str(' and ')+ str(s_o) +' and '+ str(s_e)
	return
#sc = 	init()
#startWorkingsnapshots(sc)
#automation(spark=sc)
#testFunction(sc)
#mainOps(spark = sc)
#startFilteringstep(sc)
#testVersion(spark= sc)
#sc.stop()
