#!/usr/bin/env python
import os
import sys
from pyspark import SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.sql import Row, HiveContext
from dateutil import parser
from pyspark.sql.types import *

def createSchemafromRdd(sc_ = None, getRdd = None):
	schemaString = "ID APPID FINDSTATUS_START FINDSTATUS_END UPC_SEARCHSTATUS_START UPC_SEARCHSTATUS_END HIVE_SEARCH_START HIVE_SEARCH_END ANALYTICS_START ANALYTICS_END"
	table_ = getRdd.map(lambda x : (x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9]))
	schemaString = schemaString.split(' ')
	fields = [StructField(field_name,StringType(),True) for field_name in schemaString]
	schema = StructType(fields)
	schemaTable = sc_.createDataFrame(table_,schema)
	return schemaTable

def readDataFromCsvFile(filename = None,sqlContext = None):
    
    try:
        rdd1 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "false").option("delimiter","\t").option("quote","\\").option("codec","gzip").load(filename)
        return rdd1
    except (OSError,IOError) as e :
        print e
        print('No file found ' + str(ifile))
        return None
    return  None

def init():
    sdf_props ={'user':'root','password':'My$qlD','driver':'com.mysql.jdbc.Driver'}
    return sdf_props

def main():
    props = init()
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    rdd =  readDataFromCsvFile(filename = '/npd/test/dummy.csv', sqlContext = sqlContext)
    rdd1 = rdd.rdd.map(lambda  x : Row(ID = x[0],APPID = x[1],FINDSTATUS_START = x[2],FINDSTATUS_END=x[3],UPC_SEARCHSTATUS_START=x[4],UPC_SEARCHSTATUS_END = x[5],HIVE_SEARCH_START = x[6],HIVE_SEARCH_END = x[7],ANALYTICS_START = x[8],ANALYTICS_END = x[9]))
	    
    rdd2 = sqlContext.createDataFrame(rdd1)
    #rdd2 = createSchemafromRdd(sc_ = sqlContext,getRdd = rdd.rdd) 
    rdd2.write.jdbc(url='jdbc:mysql://lslhdpcd1:3306/PACEDICTIONARY?serverTimezone=UTC',table = 'TIMETRACKER',mode = 'append',properties = props)
    #rdd2.insertInto(props,"TIMETRACKER")

    return

if __name__ == '__main__':
	main()
