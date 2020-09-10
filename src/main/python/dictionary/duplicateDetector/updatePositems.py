from processingUnit import transferSnapshotsmulti
from processingUnit import init
from processingUnit import dofiltering
from processingUnit import writeDown
from processingUnit import transferSnapshots
from processingUnit import readrddFromcsvfile
from processingUnit import readrddFromcsvfiles
from processingUnit import executeScripts
from processingUnit import hiveExecutecommands
from processingUnit import readFiles
from processingUnit import printwithstats
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
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from collections import deque
from hdfsFileDiscovery import todaysKey
from pyspark.sql import Row
from pyspark.sql.functions import avg, udf, col
from pyspark.sql.functions import to_timestamp
from operator import add
from runningHadoopcmd import run_cmd
#from readAndload import isSnapshotfiles
from map_generator import generateUPCmap
from map_generator import generateSKUmap
from map_generator import generateMODELmap
from hivewritebackModule import hivewriteback
import sys
import os
import MySQLdb
import pyspark.sql.functions as psf
from collections import defaultdict
from hdfsFileDiscovery import rangeOfdate
import math
#from processingUnit import hiveExecutecommands
def startReadingfromhdfs(listOffiles = None,sqlc = None,spark = None,multi = 0):
	data = sqlc.createDataFrame(spark.emptyRDD(),StructType([]))
	if multi is 1:
		data = transferSnapshotsmulti(sqlc,listOffiles)#.repartition(500)
	elif multi is 2:
		data = transferSnapshots1(sqlc, listOffiles, spark)#.repartition(500)
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
def startReadingfromhive(query = None, hc = None, sqlc = None, spark = None ):
	data = hiveExecutecommands(line = query,hive_context = hc).repartition(500)
	return data

def isSnapshotfiles(dates, listOfdates, debug = False):
	if debug is True:
		print "type of dates: " + str(type(dates))
		print "type of listOfdates: " + str(type(listOfdates)) 
	for each in listOfdates:
		if each == dates:
			return True
	return False

def startOverlapdetector(rddData, l1, sqlc, hc, spark):
	listFiles = listOffiles()
	listFiles = l1 + listFiles
	uniPoiid = None
	if hc is not None:
		if rddData.rdd.isEmpty() == False  :
			#print "Wrting down unique rdd data"
			rddData = rddData.distinct()
			uniPoiid = rddData
			uniPoiid = rddData.groupBy("itemid").agg(psf.max("updated").alias("updated_")).distinct()
			uniPoiid = uniPoiid.selectExpr(["itemid as itemid_", "updated_ as updated_"])
			uniPoiid = uniPoiid.withColumn("itemid_",uniPoiid["itemid_"].cast("long"))
			uniPoiid.repartition(400)
			uniPoiid.persist()
			#print "size of unique snapshots: " + str(uniPoiid.count())
			#uniPoiid.select("updated_").show()
			rddData = rddData.withColumn("itemid",rddData["itemid"].cast("long"))
			#timeFmt="yyyy-mm-dd'T'HH:mm:ss"
			timeFmt="yyyy-mm-dd HH:mm:ss"
			timeDiff = psf.abs(psf.unix_timestamp("rddData.updated", format = timeFmt) - psf.unix_timestamp("unirdd.updated_", format = timeFmt))
			rddData.repartition(400)
			rddData.persist()
			rddData = rddData.alias("rddData").join(uniPoiid.alias("unirdd"),psf.col("rddData.itemid") == psf.col("unirdd.itemid_"),'inner').where(timeDiff == 0)
			droplist = ['itemid_','updated_']
			rddData = rddData.select([column for column in rddData.columns if column not in droplist])
			#print "Returned Rdd size is :" + str(rddData.count())
			return rddData
	else:
		snapshotRdd.registerTempTable("snapshotRdd")
		print "Getting distinct partition names"
		uniquepoiidRdd = sqlc.sql("select poi_id, max(distinct updated)  as updated from snapshotRdd group by poi_id") 
		base.withColumn("updated",base["updated"].cast("string"))
		cond = [snapshotRdd.poiid  == uniquepoiidRdd.poiid, abs(unix_timestamp(snapshotRdd.withColumn("updated",snapshotRdd["updated"].cast("string"))) - unix_timestamp(uniquepoiidRdd.withColumn("updated", uniquepoiidRdd["updated"].cast("string")))) == 0]
		snapshotRdd = snapshotRdd.alias('snapshotRdd').join(uniquepoiidRdd,cond,'inner')
		sqlc.dropTempTable("snapshotRdd")
		if snapshotRdd.rdd.isEmpty() is False:
			print "size of snapshot Rdd: " + str(snapshotRdd.count())
			return snapshotRdd

	return sqlc.createDataFrame(spark.emptyRDD(),StructType([]))

def startFilteringfrombase(base,snapshot):
	base = base.filter(base.poi_id != snapshot.poi_id)
	return base
def getColumnsnameods(with1 = 0):
	if with1 is 0:
		listNames = ["itemid", "businessid", "subcategoryn", "itemnumber", "unitspackage","fld01", "fld02", "fld03", "fld04", "fld05", "fld06", "fld07", "fld08", "fld09", "fld10", "fld11", "fld12", "fld13", "fld14", "fld15", "fld16", "fld17", "fld18", "fld19", "fld20", "fld21", "fld22", "fld23", "fld24", "fld25", "fld26", "fld27", "fld28", "fld29", "fld30", "fld31", "fld32", "fld33", "fld34", "fld35", "fld36", "fld37", "fld38", "fld39", "fld40", "fld41", "fld42", "fld43", "fld44", "fld45", "fld46", "fld47", "fld48", "fld49", "fld50", "fld51", "fld52", "fld53", "fld54", "fld55", "fld56", "fld57", "fld58", "fld59", "fld60", "fld61", "fld62", "fld63", "fld64", "fld65", "fld66", "fld67", "fld68", "fld69", "fld70", "fld71", "fld72", "fld73", "fld74", "fld75", "fld76", "fld77", "fld78", "fld79", "fld80", "fld81", "fld82", "fld83", "fld84", "fld85", "fld86", "fld87", "fld88", "fld89", "fld90", "fld91", "fld92", "fld93", "fld94", "fld95", "fld96", "fld97", "fld98", "fld99", "status", "added", "updated", "vfld01", "vfld02", "vfld03", "vfld04", "vfld05", "country_code", "groupitemid", "parentitemid", "parentitemid_status", "outletitem_map_change_date", "lockdown_status"] 
	else:
		listNames = [ "ITEMID1", "BUSINESSID1", "SUBCATEGORYN1", "ITEMNUMBER1", "UNITSPERPACKAGE1", "FLD01", "FLD02", "FLD03", "FLD04", "FLD05", "FLD06", "FLD07", "FLD08", "FLD09", "FLD10", "FLD11", "FLD12", "FLD13", "FLD14", "FLD15", "FLD16", "FLD17", "FLD18", "FLD19", "FLD20", "FLD21", "FLD22", "FLD23", "FLD24","FLD25", "FLD26", "FLD27", "FLD28", "FLD29", "FLD30", "FLD31", "FLD32", "FLD33", "FLD34", "FLD35", "FLD36", "FLD37", "FLD38", "FLD39", "FLD40", "FLD41", "FLD42", "FLD43", "FLD44", "FLD45", "FLD46", "FLD47", "FLD48", "FLD49", "FLD50", "FLD51", "FLD52", "FLD53", "FLD54", "FLD55", "FLD56", "FLD57", "FLD58", "FLD59", "FLD60", "FLD61", "FLD62", "FLD63", "FLD64", "FLD65", "FLD66", "FLD67", "FLD68", "FLD69", "FLD70", "FLD71", "FLD72", "FLD73", "FLD74", "FLD75", "FLD76", "FLD77", "FLD78", "FLD79", "FLD80", "FLD81", "FLD82", "FLD83", "FLD84", "FLD85", "FLD86", "FLD87", "FLD88", "FLD89", "FLD90", "FLD91", "FLD92", "FLD93", "FLD94", "FLD95", "FLD96", "FLD97", "FLD98", "FLD99", "STATUS1", "ADDED1", "UPDATED1", "VFLD01", "VFLD02", "VFLD03", "VFLD04", "VFLD05", "COUNTRY_CODE1", "GROUPITEMID1", "PARENTITEMID1", "PARENTITEMID_STATUS1", "OUTLETITEM_MAP_CHANGE_DATE1", "LOCKDOWN_STATUS1"]
	listNames = [each.lower() for each in listNames]
	return listNames
def columnRenaming(listNames):
    cols = []
    for ind,each in enumerate(listNames):
    	cols.append('_c'+str(ind)+ ' as '  + listNames[ind])
    return cols
def getLastfilenumber(filenames = None,rw = 0,file_num_ = None, debug = False, rev = True):
	file_number  = None
	parentPath ='src/main/python/dictionary/configuration/'
	needToread = []
	if rw is 0:
	   with open( parentPath + 'lastfilenumber') as filereader:
	      file_number = filereader.readline()
	   filenames = sorted(filenames,reverse = rev)
	   if debug is True:
		   print "Inside getLastfilenumber function"
	           for each in filenames:
			for sub_each in each[1]:
				print "day: " + str(each[0]) + " list_of_files: " + str(sub_each)
	   for each in filenames:
	      if (each.split('/')[-1] > file_number):
	         needToread.append(each)
	   file_number = filenames[-1].split("/")[-1]
	   last_file_number = filenames[0].split("/")[-1]
        else:
	   with open( parentPath + 'lastfilenumberOds','w+') as datawriter:
              if file_num_ is not None:
		 print "last file name: " + file_num_
	         datawriter.write(str(file_num_))
	   return None, None
		
	return needToread, last_file_number, file_number
def writebackTohive(dictDataframe, writebackType = 0, table_name = None):
	modeType = "overwrite" if writebackType is 0 else "append"
	dictDataframe.repartition("businessid").write.mode(modeType).partitionBy("businessid").format("orc").saveAsTable(table_name)
	return

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

def readsnapshotBasetoupdate(spark = None, ranges = 2, lastFilenumber = None, configOb = None, table_index = 3, filtering = 0):
	print "initiate programe"
	quiet_logs(sc_ = spark)
	print("Start reading text data")
	hc, sqlc = hiveInit(sc_ = spark)
	bis_list = list(configOb.businessSets) if configOb is not None else None
	#print configOb.hivedbOb.get_tabNames(dbName_ = configOb.hivedbOb.get_dbNames(index = 0), index = 2)
	table_name = configOb.hivedbOb.get_dbName(index = 0) + "." + configOb.hivedbOb.get_tabNames(dbName_ = configOb.hivedbOb.get_dbName(index = 0), index = table_index )
	print table_name
	groupedDict = globalMain(pathName = configOb.input_path[3] , sc_= spark)
	snapshotIndex = 1
	date_list = []
	date_list, hr_ = rangeOfdate(ranges) if ranges is not 0 else todaysKey(delta = configOb.delta)
	print "dates are: " + str(date_list)
	totalsnapShots = sqlc.createDataFrame(spark.emptyRDD(),StructType([]))
	fileList = []
	fileList_for_base = []
	final_rdd = None
	for each in groupedDict.iteritems():
		if isSnapshotfiles(each[0].split()[0],date_list ) is False :
			fileList_for_base = fileList_for_base + each[1]
			continue
		fileList = fileList + each[1]
	
	printwithstats(fileList)
	if len(fileList) is 0:
		print "snapshot files for odsitems are empty"
		print "Its empty exiting"
		exit(0)

	print ("Get the filtered files to read")
	needToread,last_file_num, fn = getLastfilenumber(fileList)
	print "last snapshot file number: " + str(fn)
	fileList_for_base = filterBasefiles(fileList_for_base, lastfileNumber = fn)
	print "last file number for snapshot: " + str(last_file_num)
	_, last_file_num_for_base, _ = getLastfilenumber(fileList_for_base, rev = True)
	print "last base file number : " + str(last_file_num_for_base)
	print ("start reading snapshot files")
	snapshotRdd = startReadingfromhdfs(sqlc = sqlc,listOffiles = needToread,multi = 2, spark = spark)
	query = "select distinct ITEMID, BUSINESSID, SUBCATEGORYN, ITEMNUMBER, UNITSPACKAGE, FLD01, FLD02, FLD03, FLD04, FLD05, FLD06, FLD07, FLD08, FLD09, FLD10, FLD11, FLD12, FLD13, FLD14, FLD15, FLD16, FLD17, FLD18, FLD19, FLD20, FLD21, FLD22, FLD23, FLD24,FLD25, FLD26, FLD27, FLD28, FLD29, FLD30, FLD31, FLD32, FLD33, FLD34, FLD35, FLD36, FLD37, FLD38, FLD39, FLD40, FLD41, FLD42, FLD43, FLD44, FLD45, FLD46, FLD47, FLD48, FLD49, FLD50, FLD51, FLD52, FLD53, FLD54, FLD55, FLD56, FLD57, FLD58, FLD59, FLD60, FLD61, FLD62, FLD63, FLD64, FLD65, FLD66, FLD67, FLD68, FLD69, FLD70, FLD71, FLD72, FLD73, FLD74, FLD75, FLD76, FLD77, FLD78, FLD79, FLD80, FLD81, FLD82, FLD83, FLD84, FLD85, FLD86, FLD87, FLD88, FLD89, FLD90, FLD91, FLD92, FLD93, FLD94, FLD95, FLD96, FLD97, FLD98, FLD99, STATUS, ADDED, UPDATED, VFLD01, VFLD02, VFLD03, VFLD04, VFLD05, COUNTRY_CODE, GROUPITEMID, PARENTITEMID, PARENTITEMID_STATUS, OUTLETITEM_MAP_CHANGE_DATE, LOCKDOWN_STATUS from dqdictionaryhivedb.uniqueodspositems2_int" 
	bis_list = [str(each) for each in bis_list] if bis_list is not None else None
	query = query + " where BUSINESSID in (" + ','.join(bis_list) + ")" if configOb is not None else query
	baseDict = startReadingfromhdfs(sqlc = sqlc,listOffiles = fileList_for_base , multi = 2, spark = spark) if configOb.read_hive_odsitem is 0 else startReadingfromhive(query = query, hc = hc, sqlc = sqlc, spark = spark)
	listofC = getColumnsnameods()
	cols = columnRenaming(listofC)
	baseDict = baseDict.selectExpr(cols) if configOb.read_hive_odsitem is 0 else baseDict
	snapshotRdd = snapshotRdd.selectExpr(cols)
	bis_list = [ int(each) for each in bis_list] if bis_list is not None else None
	snapshotRdd = snapshotRdd.where(snapshotRdd.businessid.isin(set(bis_list))) if filtering is 1 else snapshotRdd
	baseDict = baseDict.where(baseDict.businessid.isin(set(bis_list))) if filtering is 1 else baseDict
	#baseDict = baseDict.withColumn("added",to_timestamp("added","yyyy_MM_dd hh_mm_ss"))
	#baseDict = baseDict.withColumn("updated",to_timestamp("updated","yyyy_MM_dd hh_mm_ss"))
	#baseDict = baseDict.withColumn("outletitem_map_change_date",to_timestamp("outletitem_map_change_date","yyyy_MM_dd hh_mm_ss"))
	print "snapshot data count started"
	startWorkingsnapshots(snapshotRdd = snapshotRdd, baseDict = baseDict, spark = spark, ranges = ranges, process_dict = 1, dict_hdfs = 0, dict_hive = configOb.read_hive_odsitem , writebackType = 0, debug = 0,fileList = fileList,lastFilenumber = lastFilenumber, table_name = table_name, hdfs_output = configOb.input_path[1], writeTohdfs = 0, append_in_hive = configOb.append_in_hive, updatehivetable = configOb.stage['updatehivetable'])
	return
def stringTotimestamp(rdd, columns = [], formats = 'yyyy_MM_dd', types = 'int'):
        #to_timestamp('dt_col','yyyy_MM_dd_hh_mm_ss')
	for each in columns:
		rdd = rdd.withColumn(each, rdd[each].cast(types))
	return rdd

def startWorkingsnapshots(snapshotRdd = None , baseDict = None, spark = None, ranges = 2, process_dict = 1, dict_hdfs = 0, dict_hive = 1, writebackType = 0, debug = 0,fileList = None, lastFilenumber = None, table_name = None, hdfs_output= '/npd/s_test2/uniqueOdsitems/', writeTohdfs = 0, append_in_hive = 0, updatehivetable = 0):
	#quiet_logs(sc_ = spark)
	hc, sqlc = hiveInit(sc_ = spark)
	snapshotIndex = 1
	totalsnapShots = sqlc.createDataFrame(spark.emptyRDD(),StructType([]))
	if len(fileList) is 0:
		print "snapshot files are empty"
		print "Its empty exiting"
		exit(0)
	print ("Get the filtered files to read")
	needToread, last_file_num, fn = getLastfilenumber(fileList)
	if debug is True:
		snapshot_size = snapshotRdd.count()
		print("Total data inside the snapshot rdd: " + str(snapshot_size))
		print "Total data inside base dict: " + str(baseDict.count())
	if snapshotRdd.rdd.isEmpty() is True:
		print("Calling off the operation nothing to work on snapshot is empty")
		print ("look into lastfile number all the files might have been processed")
		print "application is exiting gracefully ....."
		sys.exit(0)
	print ("Start detecting overlap data and return uniques")
	final_rdd = startOverlapdetector(snapshotRdd, ['src/main/python/dictionary/fileSource/hivecreateScripts/createExternalTable.sql'],sqlc, hc,spark)
	baseDict = startOverlapdetector(baseDict, ['src/main/python/dictionary/fileSource/hivecreateScripts/createExttabledictbase.sql'], sqlc, hc, spark) if process_dict is 1 else baseDict
	print "End of overlap detection of given snapshot rdd"
	if final_rdd.rdd.isEmpty() is True:
		print "snapshot rdd  is empty"	
		sys.exit(0)
	print ("Type cast the updated and added date column")
	final_rdd = final_rdd.withColumn("updated",final_rdd["updated"].cast("string"))
	final_rdd = final_rdd.withColumn("added",final_rdd["added"].cast("string"))
	print ("read the base dictionary")
	########### Reading unique base dictionary using spark csv reader ########################
        ################# reading unique base dictionary using hive external table #######################################
    	#baseDict = hiveExecutecommands(line = " select * from dqdictionaryhivedb.mainTemptableextpersist",hive_context = hc )
	if debug is 1:
		print ("base dictionary size " + str(baseDict.count()))
		print ("new add size: " + str(final_rdd.count()))
		print ("final_rdd size before left anti join " + str(final_rdd.count()))
	print ("perform left anti join on itemid to retrieve unique itemid based records")
	baseDict = baseDict.withColumn("itemid",baseDict["itemid"].cast("long"))
	final_rdd = final_rdd.withColumn("itemid",final_rdd["itemid"].cast("long"))
	#final_rdd = final_rdd.withColumn("added",to_timestamp("added","yyyy_MM_dd hh_mm_ss"))
	#final_rdd = final_rdd.withColumn("updated",to_timestamp("updated","yyyy_MM_dd hh_mm_ss"))
	#final_rdd = final_rdd.withColumn("outletitem_map_change_date",to_timestamp("outletitem_map_change_date","yyyy_MM_dd hh_mm_ss"))
	baseDict.persist()
	final_rdd.persist()
	if debug is 1:
		print "size of the base dictionary after adding the partitioner column: " + str(baseDict.count())
	baseDict = baseDict.alias("basetemp").join(final_rdd.alias("finalrddtemp"), (psf.col("basetemp.itemid") == psf.col("finalrddtemp.itemid")),"leftanti")
	#baseDict = baseDict.alias("basetemp").join(final_rdd.alias("finalrddtemp"),condition_list,"leftanti")
	#droplist = ['vitemid']
	#baseDict = baseDict.select([column for column in baseDict.columns if column not in droplist])
	if debug is 1:
		print ("final base dict size after left anti join " + str(baseDict.count()))
	baseDict = baseDict.unionAll(final_rdd) if append_in_hive is 0 else baseDict
	if debug is 1:
		print ("final base dict size after left anti join and union of new add all " + str(baseDict.count()))
	#baseDict = baseDict.unionAll(final_rdd)
	if writeTohdfs is 1:
		deletePath(hdfs_output,sc_ = spark)
		writeDown(baseDict, hdfs_output)
	print "Update lastfile read number"
	getLastfilenumber(rw = 1, file_num_ = last_file_num)
	#hiveExecutecommands(line = "drop table dqdictionaryhivedb.uniqueodsitems_int",hive_context = hc)
	print "Start writing back to hive table"
	baseDict = stringTotimestamp(baseDict, columns = ['added','updated'], types = 'timestamp')
	#if append_in_hive is 0 :
	#	writebackTohive(baseDict, writebackType = writebackType, table_name = table_name )
	if updatehivetable is 1 :
		print "Get a hive write back object to write backe to hdfs"
		hivewritingback  = hivewriteback(spark = spark)
		hivewritingback.setTablename(tableName = table_name )
		#hivewritingback.insertIntopartitiontable(partitionFiles = listOffiles , dictRdd = baseDict, table_name = table_name)
		hivewritingback.insertIntobucketedtable(partitionFiles = listOffiles, dictRdd = baseDict, append = 0 if append_in_hive is 0 else 1 , table_name = table_name , numberOfbus = 4, cols = ["businessid"], hc = hc )
	#
	print "End of hive transfer"
	return "Successful update of odspositem table"
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

def updateOdsitem(snapshotRdd = None,baseDict = None, sc = None , writebt = 0,process_dict = 0, appendMode = 0, ranges = 2, fileList = None, lastFilenumber = None, configOb = None ,table_name = None, hdfs_output = '/npd/s_test2/uniqueOdsitems' , writeTohdfs = 0):
	if snapshotRdd is None:
		print "If daily snapshot is None then base dictionary does not need to be updated"
		print "Daily snapshot can not be None something is wrong"
		print "So the application is exiting"
		sys.exit()
	table_name = configOb.hivedbOb.get_dbName(index = 0) + "." + configOb.hivedbOb.get_tabNames(dbName_ = configOb.hivedbOb.get_dbName(index = 0), index = 3 ) if table_name is None else table_name
	message = startWorkingsnapshots(snapshotRdd = snapshotRdd, baseDict = baseDict , spark = sc , process_dict = process_dict , writebackType = writebt, ranges = ranges, fileList = fileList, lastFilenumber = lastFilenumber, table_name = table_name , hdfs_output = hdfs_output, append_in_hive = configOb.append_in_hive, updatehivetable = configOb.stage['updatehivetable'] )
	return message
#automation(spark=sc)
#testFunction(sc)
#mainOps(spark = sc)
#startFilteringstep(sc)
#testVersion(spark= sc)
#sc.stop()
