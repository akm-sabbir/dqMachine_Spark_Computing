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
from hdfsFileDiscovery import deletePath
from updatePositems import startReadingfromhdfs
from map_generator import generateMaps
from processingUnit2 import getColumnsname
from processingUnit2 import columnRenaming
def hiveInit(sc_ = None):
	if  sc_ is None:
		print "Spark Context need to be initialized befor anything can be done"
		raise ValueError
	hive_context = HiveContext(sc_)	
	sql_context = SQLContext(sc_)
	return (hive_context, sql_context)

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


def writeDown(rddData,writingPath,append = 0):
	try:
		if append is 0:
			rddData.repartition(400).write.mode("overwrite").format("com.databricks.spark.csv").option("header","true").option("delimiter","|").option("quote","\\").option("inferSchema","true").option("codec","bzip2").option("treatEmptyValuesAsNulls","true").save(writingPath)	
		else:
			pass
			#rddData.write.mode("append").format("com.databricks.spark.csv").option("quote","\\").option("inferSchema","true").option("header","true").option("delimiter","|").option("codec","bzip2").option("treatEmptyValuesAsNulls","true").save(writingPath)	

	except (OSError, IOError) as err:
		print("final hive search is empty")
		print ("N history exist for this serach must be new adds")
		return None
	return "great"
def transferData(writebackType = 0,listNames = None,table_posoutlet = 'dqdictionaryhivedb.posoutletwithpartition_ext',hive_context = None):
	rdd = hiveExecutecommands(line = "select " + listNames + " from " + table_posoutlet, hive_context = hive_context)
        modeType = "overwrite" if writebackType is 0 else "append"
	print "End of loading data"
	rdd = rdd.repartition("business_id","partitioner")
	print "End of repartitioning"
        rdd.write.mode(modeType).partitionBy("business_id","partitioner").format("orc").saveAsTable("dqdictionaryhivedb.uniqueodsposoutlet2_int")
	print "end of transfering data"
        return
# To transfer the data set the following parameters
# readHdfs is 0
# writedown is 1
#processing table can be odsitem or odspos
# gen_map is 0
# databasename is dqdictionaryhivedb
# set the path_name1 to output directory for posoutlettable
# set the path_name2 to output directory for odsitem table
# provide configOb 

# To generate the map set the parameters as following ways
# set writedown to 0
# set gen_map to 1
# set upcM to 1 if upc map is necessary to generate
# set skuM to 1 if sku map is necessary to generate
# set processing_tab to odspos 
# provide configOb
# no concern about path_names1 and path_names2
def mainOps(spark , readHdfs = 0, writedown = 0, path_names1 = '/npd/s_test2/uniqueBasedictionary', path_names2 = '/npd/s_test2/uniqueOdsitems', table_odsitems = 'uniqueodspositems_int', table_posoutlet = 'uniqueodsposoutlet2_int',databasename = 'dqdictionaryhivedb', upcM = 0, skuM = 0, gen_map = 0, processing_tab = 'odsitem', configOb = None, debug = False):
	table_odsitems = databasename + '.' + table_odsitems if table_odsitems.find('dqdictionaryhivedb') is -1 else table_odsitems
	table_posoutlet = databasename + '.' + table_posoutlet if table_posoutlet.find('dqdictionaryhivedb') is -1 else table_posoutlet
	
	print "Start transfering the data from internal table to hdfs"
	listNames = "poi_id ,business_id ,posoutlet ,outletdivision  ,outletdepartment ,outletsubdepartment ,outletclass ,outletsubclass ,outletbrand ,outletitemnumber,outletdescription ,outletbrandmatch ,outletitemnumbermatch ,outletdescriptionmatch ,sku ,manufacturercodetype ,manufacturercode ,zzzppmonthfrom ,zzzppmonthto , zzzppmonthlastused ,itemid ,itemtype ,price ,manufacturercodestatus ,loadid ,status ,added ,updated,ppweekfrom ,ppweekto ,ppweeklastused ,matched_country_code ,previous_poiid ,include_data_ppmonthfrom ,include_data_ppweekfrom ,manufacturercodematch ,skumatch , unitofmeasure ,packsize ,manufacturername ,manufacturernamematch ,privatelabel ,outletdescriptionsupplement ,total_confidence_score , parent_poiid ,parent_poiid_status, partitioner"
	listNames2 = ["poi_id" ,"business_id" ,"posoutlet" ,"outletdivision"  ,"outletdepartment" ,"outletsubdepartment" ,"outletclass" ,"outletsubclass" ,"outletbrand" ,"outletitemnumber","outletdescription" ,"outletbrandmatch" ,"outletitemnumbermatch" ,"outletdescriptionmatch" ,"sku" ,"manufacturercodetype" ,"manufacturercode" ,"zzzppmonthfrom" ,"zzzppmonthto" , "zzzppmonthlastused" ,"itemid" ,"itemtype" ,"price" ,"manufacturercodestatus" ,"loadid" ,"status" ,"added" ,"updated","ppweekfrom" ,"ppweekto" ,"ppweeklastused" ,"matched_country_code" ,"previous_poiid" ,"include_data_ppmonthfrom" ,"include_data_ppweekfrom" ,"manufacturercodematch" ,"skumatch" , "unitofmeasure" ,"packsize" ,"manufacturername" ,"manufacturernamematch" ,"privatelabel" ,"outletdescriptionsupplement" ,"total_confidence_score" , "parent_poiid" ,"parent_poiid_status","partitioner"]
        odslistNames2 = ["itemid", "businessid", "subcategoryn", "itemnumber", "unitsperpackage","fld01", "fld02", "fld03", "fld04", "fld05", "fld06", "fld07", "fld08", "fld09", "fld10", "fld11", "fld12", "fld13", "fld14", "fld15", "fld16", "fld17", "fld18", "fld19", "fld20", "fld21", "fld22", "fld23", "fld24", "fld25", "fld26", "fld27", "fld28", "fld29", "fld30", "fld31", "fld32", "fld33", "fld34", "fld35", "fld36", "fld37", "fld38", "fld39", "fld40", "fld41", "fld42", "fld43", "fld44", "fld45", "fld46", "fld47", "fld48", "fld49", "fld50", "fld51", "fld52", "fld53", "fld54", "fld55", "fld56", "fld57", "fld58", "fld59", "fld60", "fld61", "fld62", "fld63", "fld64", "fld65", "fld66", "fld67", "fld68", "fld69", "fld70", "fld71", "fld72", "fld73", "fld74", "fld75", "fld76", "fld77", "fld78", "fld79", "fld80", "fld81", "fld82", "fld83", "fld84", "fld85", "fld86", "fld87", "fld88", "fld89", "fld90", "fld91", "fld92", "fld93", "fld94", "fld95", "fld96", "fld97", "fld98", "fld99", "status", "added", "updated", "vfld01", "vfld02", "vfld03", "vfld04", "vfld05", "country_code", "groupitemid", "parentitemid", "parentitemid_status", "outletitem_map_change_date", "lockdown_status"]
        odslistNames = "itemid, businessid, subcategoryn, itemnumber, unitspackage,fld01, fld02, fld03, fld04, fld05, fld06, fld07, fld08, fld09, fld10, fld11, fld12, fld13, fld14, fld15, fld16, fld17, fld18, fld19, fld20, fld21, fld22, fld23, fld24, fld25, fld26, fld27, fld28, fld29, fld30, fld31, fld32, fld33, fld34, fld35, fld36, fld37, fld38, fld39, fld40, fld41, fld42, fld43, fld44, fld45, fld46, fld47, fld48, fld49, fld50, fld51, fld52, fld53, fld54, fld55, fld56, fld57, fld58, fld59, fld60, fld61, fld62, fld63, fld64, fld65, fld66, fld67, fld68, fld69, fld70, fld71, fld72, fld73, fld74, fld75, fld76, fld77, fld78, fld79, fld80, fld81, fld82, fld83, fld84, fld85, fld86, fld87, fld88, fld89, fld90, fld91, fld92, fld93, fld94, fld95, fld96, fld97, fld98, fld99, status, added, updated, vfld01, vfld02, vfld03, vfld04, vfld05, country_code, groupitemid, parentitemid, parentitemid_status, outletitem_map_change_date, lockdown_status"
	hc , sqlc = hiveInit(spark)
	#transferData(listNames = listNames, hive_context = hc)
	#sys.exit(0)
	listOffiles = path_names1 if processing_tab is 'odspos' else path_names2
	line = "select " + listNames + " from " + table_posoutlet if processing_tab is "odspos" else "select " + odslistNames + " from " + table_odsitems
	rddData = startReadingfromhdfs( listOffiles = listOffiles, sqlc = sqlc, spark = spark, multi = 1) if readHdfs is 1 else hiveExecutecommands(line = line , hive_context = hc)
	#rddData.select("manufacturercode","itemid").show()
	headers = columnRenaming(listNames2) if readHdfs is 1 and processing_tab is 'odspos' else columnRenaming(odslistNames2) if readHdfs is 1 and processing_tab is 'odsitem' else [] 
	rddData = rddData.selectExpr(headers) if len(headers) is not 0 else rddData
	print "start generating the maps from odsposoutlet table"
	if debug is True:
		print "couting is an expensive operation in distributive system"
		print "total data from the table or directory: " + str(rddData.count())
	generateMaps(baseDict = rddData, spark = spark,upcM = upcM, skuM = skuM, upc_map_dir = configOb.root_mapper['upclim'],  sku_map_dir = configOb.root_mapper['skulim'] ) if gen_map is 1 else None
	if writedown is 1:
		print "start writing down the dictionary in hdfs"
	 	print "writing into: " + path_names1 if processing_tab is 'odspos' else path_names2
		deletePath(path_names1 if processing_tab is 'odspos' else path_names2, spark)
		writeDown(rddData, path_names1 if processing_tab is 'odspos' else path_names2 )
		print "End of transfering the data"
	else:
		print "we skipped the writing down the data"
		print "End of map generation"
	if processing_tab is 'odspos':
		return rddData
	else:
		return None
	return

## this is the end  of data transfer module
