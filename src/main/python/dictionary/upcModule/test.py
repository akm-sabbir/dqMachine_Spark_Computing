import sys
from collections import namedtuple

from pyspark.sql import SQLContext
from pyspark.sql import Row

from pyspark import SparkConf
from pyspark import SparkContext

import pyspark.sql.functions as funcs

import dictstatus_db

def test():

    # ##########################
    # ##########################
    # Find our files in the DB
    # ##########################
    # ##########################

    js = dictstatus_db.dictstatus_db()
    js.set_hostname('lslhdpcd1.npd.com')
    js.set_database('PACEDICTIONARY')
    js.set_table('DICTIONARY_STATUS')
    js.set_primary_key_name('CHANGE_ID')
    js.get_setup()

    # ##########################
    # ##########################
    # Load the data
    # ##########################
    # ##########################

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    sqlContext = SQLContext(sc)

    # ##########################
    # Set how much information we want to spew
    verbosity = 1


    # ##########################
    # Filename to process, we'll take this from the
    # wrapper.
    item_type = 'ITEMID'
    filename=js.get_last_file(item_type)

    for ifile in xrange(0,3):
        filenumber = int(filename.split('-m-')[1].replace('.gz',''))+1
        filename = filename.split('-m-')[0]+'-m-'
        filename = filename+'{:05}'.format(filenumber)+'.gz'

        print('read-begin: '+str(ifile)+' '+filename)
        try:
            rdd1 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "false").option("delimiter","|").option("quote","\\").option("codec","gzip").load(filename)
        except:
            print('No file found '+str(ifile))
            continue
        print('read-end')

        rdd2 = 0
        if 'ODS_POSITEMS' in filename:
            rdd2 = rdd1.rdd.map(lambda c: Row( item_id = int(c[0]), business_id = int(c[1]), subcategory_number = int(c[2]), item_number = int(c[3]),  added_date = c[105], update_date = c[106], country_code = int(c[112]), group_item_id = int(c[113]), parent_item_id = int(c[114]), parent_item_id_status = c[115], outletitem_map_change_date = c[116]))
        else:
            rdd2 = rdd1.rdd.map(lambda c: Row( poiid = c[0], business_id = c[1], item_id = c[20], posoutlet = c[2],  outletbrand = c[8], outletitemnumber = c[9], outletdescription = c[10], sku = c[14], manufacturercode = c[16], added_date = c[26], update_date = c[27]))

        df1 = rdd2.toDF()

        if item_type=='ITEMID':#df.select(df.name.substr(1, 3)
            timeFmt = "yyyy-mm-dd'T'HH:mm:ss.SSS"
            timeScale = 86400
            timeDiff = (funcs.unix_timestamp('update_date', format=timeFmt)-funcs.unix_timestamp('added_date', format=timeFmt))/timeScale
            df1 = df1.withColumn("Duration", timeDiff)
        if item_type=='POIID':#df.select(df.name.substr(1, 3)
            df1 = df1.withColumn('update_date',df1.update_date.substr(0, 10))
            df1 = df1.withColumn("Duration", funcs.datediff('update_date','added_date'))
    

            ## filtering for the records less than 2 days
            df2 = df1.filter(df1.Duration < 2 )
            df2 = df2.filter(df2.item_id>0)

            print('collect()')
            mydict = df2.toPandas().to_dict(orient='list')
            for k,v in mydict.items():
                print(str(k)+' '+str(len(v)))

            js.start_new_changeid_dict(mydict,'NewAdd',item_type,filename)
    
        
    print('Done')
    
test()

