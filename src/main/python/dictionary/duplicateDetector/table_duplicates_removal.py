
def getColumnsnameods(with1 = 0):
	listNames = ["itemid", "businessid", "subcategoryn", "itemnumber", "unitspackage","fld01", "fld02", "fld03", "fld04", "fld05", "fld06", "fld07", "fld08", "fld09", "fld10", "fld11", "fld12", "fld13", "fld14", "fld15", "fld16", "fld17", "fld18", "fld19", "fld20", "fld21", "fld22", "fld23", "fld24", "fld25", "fld26", "fld27", "fld28", "fld29", "fld30", "fld31", "fld32", "fld33", "fld34", "fld35", "fld36", "fld37", "fld38", "fld39", "fld40", "fld41", "fld42", "fld43", "fld44", "fld45", "fld46", "fld47", "fld48", "fld49", "fld50", "fld51", "fld52", "fld53", "fld54", "fld55", "fld56", "fld57", "fld58", "fld59", "fld60", "fld61", "fld62", "fld63", "fld64", "fld65", "fld66", "fld67", "fld68", "fld69", "fld70", "fld71", "fld72", "fld73", "fld74", "fld75", "fld76", "fld77", "fld78", "fld79", "fld80", "fld81", "fld82", "fld83", "fld84", "fld85", "fld86", "fld87", "fld88", "fld89", "fld90", "fld91", "fld92", "fld93", "fld94", "fld95", "fld96", "fld97", "fld98", "fld99", "status", "added", "updated", "vfld01", "vfld02", "vfld03", "vfld04", "vfld05", "country_code", "groupitemid", "parentitemid", "parentitemid_status", "outletitem_map_change_date", "lockdown_status"] 
	return listNames

def startOverlapdetector(rddData, l1, sqlc, hc, spark, colN = None, type_ = "long"):
	listFiles = listOffiles()
	listFiles = l1 + listFiles
	uniPoiid = None
	if hc is not None:
		if rddData.rdd.isEmpty() == False  :
			#print "Wrting down unique rdd data"
			rddData = rddData.distinct()
			uniPoiid = rddData
			uniPoiid = rddData.groupBy(colN).agg(psf.max("updated").alias("updated_")).distinct()
			uniPoiid = uniPoiid.selectExpr([colN + " as " + colN +"_", "updated_ as updated_"])
			uniPoiid = uniPoiid.withColumn(colN + "_",uniPoiid[colN + "_"].cast(type_))
			#uniPoiid.repartition(400)
			uniPoiid.persist()
			#print "size of unique snapshots: " + str(uniPoiid.count())
			#uniPoiid.select("updated_").show()
			rddData = rddData.withColumn(colN, rddData[colN].cast(type_))
			#timeFmt="yyyy-mm-dd'T'HH:mm:ss"
			timeFmt = "yyyy-mm-dd HH:mm:ss"
			timeDiff = psf.abs(psf.unix_timestamp("rddData.updated", format = timeFmt) - psf.unix_timestamp("unirdd.updated_", format = timeFmt))
			#rddData.repartition(400)
			rddData.persist()
			rddData = rddData.alias("rddData").join(uniPoiid.alias("unirdd"),psf.col("rddData." + colN) == psf.col("unirdd." + colN + "_"),'inner').where(timeDiff == 0)
			droplist = [colN + '_', 'updated_']
			rddData = rddData.select([column for column in rddData.columns if column not in droplist])
			#print "Returned Rdd size is :" + str(rddData.count())
			return rddData

	return sqlc.createDataFrame(spark.emptyRDD(),StructType([]))

