import sys
from pyspark.sql import Row
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
from processingUnit import hiveExecutecommands
from processingUnit import readrddFromcsvfile
from processingUnit import readrddFromcsvfiles
from processingUnit import writeDown
from hdfsFileDiscovery import deletePath
from processingUnit import hiveInit
import pyspark.sql.functions as psf
def testing_readData(spark,hc):
	rdd = hiveExecutecommands(line = "select * from dqdictionaryhivedb.uniqueodsposoutlet2_int where business_id ", hive_context = hc)	
	return rdd
def generateUPCmap(rdddata = None):
	if rdddata is None:
		print "Rdd is None raising value error"
		raise ValueError
		sys.exit(0)
	if isinstance(rdddata,RDD) is True:
		rdddata = rdddata.toDF()
	rdddata = rdddata.repartition("manufacturercode","itemid")
	rdddata = rdddata.rdd.map(lambda x: ((x.manufacturercode ,x.itemid),1)).cache()
	rdddata = rdddata.aggregateByKey(0, lambda key,value: key + int(value) , lambda p1,p2 : p1 + p2   ).map(lambda x: Row(UPC = x[0][0], ITEMID=x[0][1], COUNT = x[1])).toDF()
	rdddata = rdddata.repartition("UPC")
	rdddataForjoin = rdddata.rdd.map(lambda x: (x.UPC,x.COUNT)).aggregateByKey(0,lambda k,v: k + int(v),lambda p1, p2 : p1 + p2 ).map(lambda x : Row(UPC_O = x[0],TOTAL= x[1])).toDF()
	rdddata = rdddata.alias('rdddata').join(rdddataForjoin.alias('joinedRdddata'),psf.col("rdddata.UPC") == psf.col("joinedRdddata.UPC_O"), 'inner').select("UPC","ITEMID","COUNT","TOTAL")
	return rdddata

def generateSKUmap(rdddata):
	if rdddata is None:
		print "Rdd is None raising value error"
		raise ValueError
		sys.exit(0)
	if isinstance(rdddata,RDD) is True:
		rdddata = rdddata.toDF()
	rdddata = rdddata.repartition("sku","posoutlet","itemid")
	rdddata = rdddata.rdd.map(lambda x: ((x.posoutlet,x.sku,x.itemid),1)).aggregateByKey(0,lambda k,v : k + int(v),lambda p1,p2 : p1 + p2 ).map(lambda x: Row(POSOUTLET = x[0][0], SKU = x[0][1], ITEMID=x[0][2], COUNT = x[1])).toDF()#.cache()
	#print "counted data inside sku map: " + str(rdddata.count())
	rdddata = rdddata.repartition("sku","posoutlet")
	rdddataForjoin = rdddata.rdd.map(lambda x: ((x.POSOUTLET,x.SKU),x.COUNT)).aggregateByKey(0,lambda k,v : k + int(v),lambda p1, p2 : p1 + p2).map(lambda x : Row(POSOUTLET_O = x[0][0],SKU_O=x[0][1],TOTAL= x[1])).toDF()
	#print "total data: " + str(rdddataForjoin.count())
	joinCond = [psf.col("rdddata.POSOUTLET") == psf.col("rdddataForjoin.POSOUTLET_O"), psf.col("rdddata.SKU") == psf.col("rdddataForjoin.SKU_O")]
	#rdddata = rdddata.withColumn('SKU',rdddata['SKU'].cast("long"))
	#rdddata = rdddata.withColumn("POSOUTLET",rdddata["POSOUTLET"].cast("long"))
	#rdddataForjoin = rdddataForjoin.withColumn("POSOUTLET_O",rdddataForjoin["POSOUTLET_O"].cast("long"))
	#rdddataForjoin = rdddataForjoin.withColumn("SKU_O",rdddataForjoin["SKU_O"].cast("long"))
	#rdddata.persist()
	#rdddataForjoin.persist()
	rdddata = rdddata.alias('rdddata').join(rdddataForjoin.alias("rdddataForjoin") , joinCond , 'inner').select("POSOUTLET","SKU","ITEMID","COUNT","TOTAL")
	#print "rdddata count after join operation: " + str(rdddata.count())
	rdddata = rdddata.rdd.map(lambda x : Row(OUTLET_SKU = str(x.POSOUTLET) if x.POSOUTLET is not None else '' + 'NPD' + str(x.SKU) if x.SKU is not None else '', ITEMID = x.ITEMID, COUNT = x.COUNT, TOTAL = x.TOTAL)).toDF()
	#print "total data after join : " + str(rdddata.count())
	rdddata = rdddata.select("OUTLET_SKU","ITEMID","COUNT","TOTAL")
	return rdddata

def generateMODELmap(rdddata):
	if rdddata is None:
		print "Rdd is None raising value error"
		raise ValueError
		sys.exit(0)
	if isinstance(rdddata,RDD) is True:
		rdddata = rdddata.toDF()
	rdddata = rdddata.rdd.map(lambda x: ((x.outletbrand,x.outletitemnumber,x.itemid),1)).reduceByKey(add).map(lambda x: Row(OUTLETBRAND = x[0][0],OUTLETITEMNUMBER = x[0][1], ITEMID=x[0][2], COUNT = x[1])).toDF().cache()
	rdddataForjoin = rdddata.rdd.map(lambda x: ((x.OUTLETBRAND,x.OUTLETITEMNUMBER),x.COUNT)).reduceByKey(add).map(lambda x : Row(OUTLETBRAND_O = x[0][0],OUTLETITEMNUMBER_O = x[0][1],TOTAL = x[1])).toDF()
	joinCond = [rdddata.OUTLETBRAND == rdddataForjoin.OUTLETBRAND_O, rdddata.OUTLETITEMNUMBER == rdddataForjoin.OUTLETITEMNUMBER_O]
	rdddata = rdddata.alias('rdddata').join(rdddataForjoin , joinCond , 'inner').select("OUTLETBRAND","OUTLETITEMNUMBER","ITEMID","COUNT","TOTAL")
	rdddata = rdddata.rdd.map(lambda x : Row(BRAND_MODEL = str(x.OUTLETBRAND) + 'NPD' + str(x.OUTLETITEMNUMBER) , ITEMID = x.ITEMID, COUNT = x.COUNT, TOTAL = x.TOTAL)).toDF()

	return rdddata

def generateMaps(baseDict = None,  spark = None,upcM = 1 ,skuM = 1,modelM = 0,root_directory1 = '/npd/test/maps/dictionary/itemid_maps/', root_directory2 = '/npd/test/maps/dictionary/itemid_maps2/', businessId = None, sku_map_dir = None, upc_map_dir = None, mod_map_dir = None):
        if baseDict.rdd.isEmpty() is True:
		print "No data to generate the mappings"
		print "System is exiting and returing"
		return "No map has been generated" 
	upc_map = generateUPCmap(rdddata = baseDict) if upcM is 1 else None
	if upc_map is not None:
		deletePath( upc_map_dir , sc_ = spark) if businessId is None else deletePath(root_directory2 + 'upc_map/' + businessId , sc_ = spark)
	path_name = root_directory2 + 'upc_map' if businessId is None else root_directory2 + 'upc_map/' + businessId
	writeDown(upc_map, upc_map_dir, partitions = 400) if upc_map is not None else None
	upc_map.unpersist() if upc_map is not None else None
	sku_map = generateSKUmap(rdddata = baseDict) if skuM is 1 else None
	if sku_map is not None:
		deletePath(sku_map_dir , sc_ = spark) if businessId is None else deletePath(root_directory2 + 'sku_map/' + businessId ,sc_ = spark)
	path_name = root_directory2 + 'sku_map/' if businessId is None else root_directory2 + 'sku_map/' + businessId
	writeDown(sku_map , sku_map_dir, partitions = 400 ) if sku_map is not None else None
	sku_map.unpersist() if sku_map is not None else None
	path_name = root_directory2 + 'model_map/' if businessId is None else root_directory2 + 'model_map/' + businessId
	model_map = generateMODELmap(rdddata = baseDict) if modelM is 1 else None
	if model_map is not None:
		deletePath(root_directory2 + 'model_map/',sc_ = spark) if businessId is None else deletePath(root_directory2 + 'model_map/' + businessId ,sc_ = spark)
	writeDown(model_map, path_name, partitions = 400 ) if model_map is not  None else None
	model_map.unpersist() if model_map is not None else None
	#rdd = testing_readData(spark , hc)
	return "End of Successful generation and write down of maps"
