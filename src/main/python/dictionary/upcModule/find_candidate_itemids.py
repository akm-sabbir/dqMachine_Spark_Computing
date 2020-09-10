import setup_db as db
from operator import is_not
from functools import partial

from pyspark import SparkConf
from pyspark import SparkContext
import appDebug
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row
import sys
from ast import literal_eval
from csv import DictReader
import csv
import pyspark.sql.functions as psf
from pyspark.sql.functions import avg, udf, col
#import org.apache.spark.sql.SaveMode
# ##########################
# Function to find corresponding ITEMIDs
# using the UPC, SKU+Outlet, Model#-Brand
# Note that this comes from a map which will need to be

def filterFunc(index, dataArr):
	
	return
def mysqlAuthenticationUsingJdbc():
	userName = 'root'
	password = 'My$qlD'
	driver = 'com.mysql.jdbc.Driver'
	url = 'jdbc:mysql://lslhdpcd1:3306/PACEDICTIONARY?serverTimezonze=UTC'
	properties = {'user':userName,'password':password,'driver':driver}
	return (url, properties)
def readMySql(sc_ = None):
	sqlJ = 'SELECT  CHANGE_ID ' +  ' FROM ' + self.table_name + '  WHERE LATESTADDFLAG = 1 and ITEM_TYPE = "POIID" '
        sql = 'SELECT UPC,SKU,POSOUTLET,OUTLETITEMNUMBER,OUTLETBRAND,PD.CHANGE_ID,DQS.CHANGE_ID FROM  POIID_DETAILS  as PD inner join (' + sqlJ + ') as DQS on (PD.LATESTADDLAG = 1 and  PD.POIID = DQS.POIID);'
	url, prop = mysqlAuthenticationUsingJdbc()
	df  = sc_.read.jdbc(url = url,dbtable=sql,properties=props)
	df = df.select("UPC","SKU","POSOUTLET","OUTLETITEMNUMBER","OUTLETBRAND")
	results = df.rdd.map(lambda x : (x.UPC,x.SKU,x.POSUTLET,x.OUTLETITEMNUMBER,x.OUTLETBRAND)).collect()
	upcs = [data if data is not None else 0 for data in list(zip(*results)[0])]
        skus = [data if data is not None else 0 for data in list(zip(*results)[1])]
        rtrs = [data if data is not None else 0 for data in list(zip(*results)[2])]
        mods = [data if data is not None else 0 for data in list(zip(*results)[3])]
        bnds = [data if data is not None else 0 for data in list(zip(*results)[4])]
	poiid_d = [data if data is not None else 0 for data in list(zip(*results)[5])]         
	pace_d = [data if data is not None else 0 for data in list(zip(*results)[6])]
	return {'UPC':upcs, 'SKU':skus, 'RTRS':rtrs, 'MOD':mods, 'BRANDS':bnds,'POIID':poiid_d,'PACED':pace_d}

def writeMySql():
	
	return
def getsplittedDataframe(dataFrame, _type = 'UPC',sqlContext = None,sc = None):
        df = sqlContext.createDataFrame(sc.emptyRDD(),StructType([]))
        df_I = sqlContext.createDataFrame(sc.emptyRDD(),StructType([]))
	if _type is 'SKU':
		try:
        		#df = dataFrame.rdd.map(lambda x :  Row(POIID = x.POIID, UPC = x.UPC, POSOUTLET=x.POSOUTLET, OUTLETBRAND=x.OUTLETBRAND, OUTLETITEMNUMBER=x.OUTLETITEMNUMBER, ITEMID = x.ITEMID, SKU = x.SKU) if x.SKU is not 'null' else None).filter(lambda x : x != None ).toDF() 
			df = dataFrame.filter(dataFrame.SKU.isNotNull())
		except ValueError as ve:
			print "It was a value error creating empty dataframe df"
			df = sqlContext.createDataFrame(sc.emptyRDD(),StructType([]))
		try:
                	#df_I = dataFrame.rdd.map(lambda x : Row(POIID = x.POIID, UPC = x.UPC , POSOUTLET = x.POSOUTLET, OUTLETBRAND = x.OUTLETBRAND, OUTLETITEMNUMBER = x.OUTLETITEMNUMBER, ITEMID = x.ITEMID) if x.SKU is 'null' else None).filter(lambda x : x != None ).toDF()
			df_I = dataFrame.filter(dataFrame.SKU.isNull())
		except ValueError as ve:
			print "It was a value error creating empty dataframe df_I"
			df_I = sqlContext.createDataFrame(sc.emptyRDD(),StructType([]))
 	if _type is 'MOD':
		try:
        		#df = dataFrame.rdd.map(lambda x : Row(POIID = x.POIID, UPC = x.UPC , POSOUTLET = x.POSOUTLET, OUTLETBRAND=x.OUTLETBRAND, OUTLETITEMNUMBER=x.OUTLETITEMNUMBER, ITEMID = x.ITEMID)  if x.OUTLETBRAND is not 'null' and x.OUTLETITEMNUMBER is not 'null' else None).filter(lambda x : x != None ).toDF() 
			df = dataFrame.filter(dataFrame.OUTLETBRAND.isNotNull())
		except ValueError as ve:
			print "It was a value error creating empty dataframe df"
			df = sqlContext.createDataFrame(sc.emptyRDD(),StructType([]))
		try:
                	df_I = dataFrame.rdd.map(lambda x : Row(POIID = x.POIID, UPC = x.UPC , POSOUTLET = x.POSOUTLET, OUTLETBRAND=x.OUTLETBRAND, OUTLETITEMNUMBER=x.OUTLETITEMNUMBER, ITEMID = x.ITEMID) if x.OUTLETBRAND is 'null' or x.OUTLETITEMNUMBER is  'null' else None).filter(lambda x : x != None ).toDF()
		except ValueError as ve:
			print "It was a value error creating empty dataframe df_I"
			df_I = sqlContext.createDataFrame(sc.emptyRDD(),StructType([]))

	return df, df_I
def find_candidate_itemids(_verbosity = 1, sc = None,itemid_f = None,poiid_f = None, remap_f = None,use_db = False,config_= None,upc_m = None, sku_m = None, model_m = None):
    # this is a local utility function which may not be necesssary
    def utility(x = None,data_key = 'upc'):
	if data_key is 'UPC':
		return	(None if x is None else x.UPC)
	elif data_key is 'SKU':
		return (None if x is None else x.POSOUTLET + 'NPD' + x.SKU)
	elif data_key is 'MOD':
		return (None if x is None else x.OUTLETBRAND + 'NPD' + x.OUTLETITEMNUMBER)
    ##################
    # Load the dictionary db
    mydb = None
    if use_db == True:
    	mydb = db.dictionary_status_setup(_verbosity)
    # modified the code in here to work for testing
    if use_db == True:
    	results = mydb.start_upc_search('UPC_SEARCH') # readMySql(sc_ = sc)
    # Load the reference datas
    # ##########################
    sc.setLogLevel("ERROR")
    # ##########################
    # Load the business map into the fession
    sqlContext = SQLContext(sc)
    usm_results = poiid_f.rdd.map(lambda x : Row(POIID = x.poi_id, UPC = x.manufacturercode, SKU = x.sku, POSOUTLET = x.posoutlet, OUTLETBRAND = x.outletbrand, OUTLETITEMNUMBER = x.outletitemnumber, ITEMID = x.itemid)).toDF()
    business_rdd = poiid_f.rdd.map(lambda x : Row(BUSINESSID = x.business_id, UPC = x.manufacturercode,POIID = x.poi_id , ITEMID = x.itemid)).toDF()

    usm_results = usm_results.withColumn("UPC",usm_results["UPC"].cast("long"))
    usm_results = usm_results.withColumn("SKU",usm_results["SKU"].cast("string"))
    usm_results = usm_results.withColumn("POIID",usm_results["POIID"].cast("long"))
    usm_results = usm_results.withColumn("POSOUTLET",usm_results["POSOUTLET"].cast("long"))
    usm_results = usm_results.withColumn("OUTLETBRAND",usm_results["OUTLETBRAND"].cast("string"))
    usm_results = usm_results.withColumn("OUTLETITEMNUMBER",usm_results["OUTLETITEMNUMBER"].cast("string"))
    usm_results = usm_results.withColumn("ITEM_ID",usm_results["ITEMID"].cast("long"))
    # ########################## 
    #usm_results_ = usm_results.rdd.map(lambda x : x if x.UPC != None else None).filter(lambda x: x != None).toDF()
    usm_results_ = usm_results.where(usm_results.UPC.isNotNull() == True)
    #usm_results_1 = usm_results.where(usm_results.UPC != None)
    #print "count of the usm results with version 1:" + str(usm_results_1.count())
    print "size of the usm results after filtering: " + str(usm_results_.count())
    sku_search = usm_results.rdd.map(lambda x: Row(POIID = x.POIID, SKU=x.SKU, POSOUTLET=x.POSOUTLET, OUTLETBRAND=x.OUTLETBRAND, OUTLETITEMNUMBER=x.OUTLETITEMNUMBER, ITEMID = x.ITEMID) if x.UPC == None else None).filter(lambda x : x != None).toDF()
    sku_search = usm_results.where(usm_results.UPC.isNull() == True)
    final_sku_search = sku_search.where(sku_search.SKU.isNotNull() == True)
    #final_sku_search = sku_search.rdd.map(lambda x : x if x.SKU is not 'null' else None).filter(lambda x : x != None ).toDF()
    mod_search = sku_search.where(sku_search.SKU.isNull() == True )#sku_search.rdd.map(lambda x : Row(POIID = x.POIID, POSOUTLET=x.POSOUTLET, OUTLETBRAND=x.OUTLETBRAND, OUTLETITEMNUMBER=x.OUTLETITEMNUMBER, ITEMID = x.ITEMID) if x.SKU is 'null' else None).filter(lambda x : x != None ).toDF() if (sku_search.count() - final_sku_search.count()) is not 0 else sqlContext.createDataFrame(sc.emptyRDD(),StructType([]))
    # this will go into log information 
    sku_count = sku_search.count()
    mod_count = mod_search.count()
    upc_count = usm_results_.count()
    poiid_count = poiid_f.count()
    print "Size of sku search data: " + str(sku_count) 
    print "Size of model search data: " + str(mod_count)
    print "Registering table for query execution"
    print "amount of data need to be searched with SKU: " + str(sku_count)
    print "size of original new add poiid " + str(poiid_count)
    print "size of original new add poiid with valid upc value: " + str(upc_count)
    #print "size of poiid_f is: " + str(poiid_f.count())
    # End of the log information 
    # this is for log information as well
    if upc_count is not 0:
        print "UPC search data displayed"
    if sku_count is not 0:
	print "Count of None or NULL value upc: " + str(usm_results_.count())
    if mod_count is not 0:
	print "display of model search based data"
    print "Detecting records with proper UPC value"
    # End of the log information here
    ###########################################################################################################
    nullUpcrecords = poiid_count - upc_count
    # this should go under log information
    print "number of records Null upc count: " + str(nullUpcrecords)
    print "Initiate UPC based search to retrieve relevant itemids"
    # End of user logging
    df_upc, df_I = process_searching( sqlContext, 'UPC', usm_results_, configOb = config_, map_ = upc_m) if upc_count is not 0 else sqlContext.createDataFrame(sc.emptyRDD(),StructType([]))
    print "Number of upc based itemid retrieved: " + str(df_upc.count())
    df_sku_I, df_mod_I = getsplittedDataframe(df_I, _type = 'SKU', sqlContext = sqlContext, sc = sc)  if df_I.count() is not 0 else(sqlContext.createDataFrame(sc.emptyRDD(),StructType([])),sqlContext.createDataFrame(sc.emptyRDD(),StructType([])))
    print "after upc-itemid mapping inverse sku based itemid counts " +str(df_sku_I.count())
    upc_list = df_upc.rdd.map(lambda x : long(x.ITEMID)).collect() if df_upc.count() is not 0 else []
    if nullUpcrecords is 0:
	print "there are no records whose upcs are NULL so search will be completed soon"
	if len(upc_list ) is not 0:
		return  upc_list, df_upc
    	else:
		print ("there is nothing to proceed and do hive search")
		sys.exit(0)
    # SKU SEARCHING
    print "Start of SKU based search operation"
    #usm_results_.registerTempTable("usm_resultsT2")
    #print "count of usm results: " + str(usm_results_.count())
    usm_results_.unpersist()
    final_sku_search = final_sku_search.unionAll(df_sku_I) if df_sku_I.count() is not 0 else final_sku_search
    mod_search = mod_search.unionAll(df_mod_I) if df_mod_I.count() is not 0 else mod_search
    df_sku, df_I = process_searching(sqlContext,'SKU', final_sku_search,configOb = config_, map_ = sku_m) if final_sku_search.count() is not 0 else  (sqlContext.createDataFrame(sc.emptyRDD(),StructType([])),sqlContext.createDataFrame(sc.emptyRDD(),StructType([])))
    #upc_list = df_sku.rdd.map(lambda x : long(x.ITEMID)).collect() + upc_list
    print  "Lenght of sku_list: " + str(df_sku.count())
    print "perform union of df_upc and sku_upc"
    df_upc = df_upc.select("ITEMID").distinct()
    df_sku = df_sku.select("ITEMID").distinct()
    df_upc = df_upc.unionAll(df_sku) if df_sku.count() is not 0 else df_upc
    #df_sku = df_sku.select("ITEMID").distinct() if df_sku.count() is not 0 else sqlContext.createDataFrame(sc.emptyRDD(),StructType([])) 
    #upc_list2 = df_sku.collect() if df_sku.count() is not 0 else []
    #upc_list = upc_list + upc_list2
    print "lenght of upc and sku combine list " + str(df_upc.count())
    if df_upc.count() is 0:
	print "there is no match in the history"
	print "May be something wrong"
	print "application is exiting here"
	sys.exit()
    df_sku.unpersist()
    df_upc = df_upc.select([each.lower() for each in df_upc.columns])
    upc_list_= df_upc.rdd.map(lambda x: x.itemid).collect()
    return upc_list_, df_upc,business_rdd
    if (poiid_f.count() - (usm_results_.count() + sku_search.count())) ==  0 :
	if len(upc_list) is not 0:
		return upc_list, df_upc
	else:
		print "Application exit here gracfully nothing to search in the history"
		print "All items are "
		return None
    # MODEL number SEARCHING
    df_mod_I, _  = getsplittedDataframe(df_I , _type = 'MOD',sqlContext = sqlContext,sc = sc) if df_I.count() is not 0 else (sqlContext.createDataFrame(sc.emptyRDD(),StructType([])),sqlContext.createDataFrame(sc.emptyRDD(),StructType([])))
    mod_search = mod_search.unionAll(df_mod_I) if df_mod_I.count() is not 0 else mod_search
    df_mod, df_I = process_search(sqlContext, 'MOD',mod_results,configOb = config_, map_ = model_m) if mod_search.count() is not 0 else sqlContext.createDataFrame(sc.emptyRDD(),StructType([])) 
    df_upc = df_upc.unionAll(df_mod) if df_mod.count() is not 0 else df_upc 
    df_upc = df_upc.rdd.map(lambda x: (x.ITEMID,x.POIID)).toDF()
    df_upc = df_upc.select("ITEMID").distinct() if df_upc.count() is not 0 else sqlContext.createDataFrame(sc.emptyRDD(),StructType([])) 
    print "Total records we have processed and candidates for hive search: " + str(df_upc.count())
    upc_list = df_upc.rdd.map(lambda x : (x.ITEMID)).collect() if df_upc.count() is not 0 else []
    print "Size of the list is: " + str(len(upc_list))
    #itemidDataframe.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "|").option("codec","bzip2").option("treatEmptyValuesAsNulls","true").save('/npd/test/ODS/s_test/itemidFrame/')
    #poiidDataframe.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "|").option("codec","bzip2").option("treatEmptyValuesAsNulls","true").save('/npd/test/ODS/s_test/poiidFrame/')  
    #remapDataframe.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "|").option("codec","bzip2").option("treatEmptyValuesAsNulls","true").save('/npd/test/ODS/s_test/remapFrame/')
    #spark.catalog.dropTempView("usm_resultsT"
    df_upc.columns = df_upc.columns.str.lower()
    df_upc.rdd.map(lambda x: x.itemid).collect()
    return upc_list, df_upc, business_rdd
    
def process_searching(_sqlContext, _type,_results,configOb = None, map_ = None):
    print('Processing ' + str(_type) + ' Search')
    print "Root directory must be the part of configuration file"
    root_directory = '/npd/test/maps/dictionary/itemid_maps/'
    print(_results)

    data_key = 'UPC'
    if 'UPC' in _type:
        root_directory = configOb.root_mapper['upclim'] #root_directory + 'upc/UPC_' 
    elif 'SKU' in _type:
        root_directory = configOb.root_mapper['skulim'] #root_directory + 'sku/SKU_'
        data_key = 'OUTLET_SKU'
    elif 'MOD' in _type:
        root_directory = configOb.root_mapper['MOD'] #root_directory + 'model/MOD_'
        data_key = 'BRAND_MODEL'
    df = None
    if  map_ is None:
    	df = _sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("codec","bzip2").option("header", "false").option("delimiter",'|').load(root_directory+'/*')
    	print _results.dtypes
    	cols = []
	if df is None or df.rdd.isEmpty() is True:
		return (sqlContext.createDataFrame(sc.emptyRDD(),StructType([])), sqlContext.createDataFrame(sc.emptyRDD(),StructType([])))
    	#cols.append('_c1 as ' + data_key)
    	#cols.append('_c2 as ITEMID')
    	#cols.append('_c3 as COUNT')
    	#cols.append('_c4 as TOTAL')
	cols.append('_c0 as ' + data_key) #if data_key is 'UPC' else cols.append('_c2 as ' + data_key)
    	cols.append('_c1 as ITEMID')
    	cols.append('_c2 as COUNT') #if data_key is 'UPC' else cols.append('_c0 as COUNT')
    	cols.append('_c3 as TOTAL')
    	df = df.selectExpr(cols)
	df.show()
    else:
	df = map_
    #df.withColumn('UPC', df['UPC'].cast('string')) 
    print df.dtypes
    print _results.dtypes
    df = df.withColumn('UPC', df['UPC'].cast('long')) if data_key is 'UPC' else df
    df = df.withColumn('OUTLET_SKU', df['OUTLET_SKU'].cast('string')) if data_key is 'SKU' else df
    df = df.withColumn('BRAND_MODEL',df['BRAND_MODEL'].cast('string')) if data_key is 'MOD' else df
    df = df.withColumn('Business', input_file_name())
    df = df.withColumn('Business', regexp_replace('Business', 'hdfs://NPDHDPSL'+root_directory, ''))
    df = df.withColumn('Business', regexp_replace('Business', '.out', ''))
    df_I = None
    if data_key == 'UPC' :
    	#print df.dtypes
    	#print _results.dtypes
	_results = _results.withColumn('UPC',_results['UPC'].cast('long'))
	df = df.withColumn('UPC',df['UPC'].cast('long'))
	df = df.alias('df').join(_results.alias('_results'), psf.col("df.UPC") == psf.col("_results.UPC"),'inner').select('df.*')
	df_I = _results.alias('_results').join(df.alias("df"), psf.col("_results.UPC") == psf.col("df.UPC"),'leftanti').select('_results.*')
	_results.unpersist()
    elif data_key == 'OUTLET_SKU':
	resultsList = _results.rdd.map(lambda x : Row(SKU_O = str(x.POSOUTLET) + 'NPD' + str(x.SKU), POIID = x.POIID, SKU = x.SKU, POSOUTLET = x.POSOUTLET, OUTLETBRAND = x.OUTLETBRAND, OUTLETITEMNUMBER = x.OUTLETITEMNUMBER, ITEMID = x.ITEMID )).toDF()
        print "count of search list :" + str(resultsList.count())
	df = df.alias('df').join(resultsList.alias('resultsList'), psf.col("df.OUTLET_SKU") == psf.col("resultsList.SKU_O") ,'inner').select('df.*')
	df_I = resultsList.alias('resultsList').join(df.alias("df"),psf.col("resultsList.SKU_O") == psf.col("df.OUTLET_SKU"),'leftanti')#.select('resultsList.POIID,resultsList.SKU,resultsList.POSOUTLET,resultsList.OUTLETBRAND,resultsList.OUTLETITEMNUMBER,resultsList.ITEMID')
        df_I = df_I.select("POIID", "SKU","POSOUTLET", "OUTLETBRAND","OUTLETITEMNUMBER","ITEMID")
    	resultsList.unpersist()
    elif data_key == 'BRAND_MODEL':
	resultsList = _results.rdd.map(lambda x : Row(OUTLETBRAND_O = x.OUTLETBRAND + 'NPD' + x.OUTLETITEMNUMBER,POIID = x.POIID,  POSOUTLET = x.POSOUTLET, OUTLETBRAND = x.OUTLETBRAND, OUTLETITEMNUMBER = x.OUTLETITEMNUMBER, ITEMID = x.ITEM_ID)).toDF()
	print "count of search list :" + str(resultsList.count())
	df = df.alias('df').join(resultsList, df.BRAND_MODEL == resultsList.OUTLETBRAND_O ,'inner').select('df.*')
        df_I = resultsList.alias('resultsList').join(df, resultsList.OUTLETBRAND_O == df.BRAND_MODEL,'leftanti').select("resultsList.POIID","resultsList.POSOUTLET","resultsList.OUTLETBRAND","resultsList.OUTLETITEMNUMBER","resultsList.ITEMID")
    	resultsList.unpersist()
    #df = df.join(_results,df.data_key == results.UPC).select([col(colN) for colN in results.columns] + [col(df.ITEMID)])
    #df.show()
    return (df,df_I)
#if __name__ == '__main__':
#find_candidate_itemids()
