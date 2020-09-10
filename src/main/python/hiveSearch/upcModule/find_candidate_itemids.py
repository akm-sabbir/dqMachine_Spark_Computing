import setup_db as db

from pyspark import SparkConf
from pyspark import SparkContext

from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

# ##########################
# ##########################
#
# Function to find corresponding ITEMIDs
# using the UPC, SKU+Outlet, Model#-Brand
# Note that this comes from a map which will need to be
# updated periodically
#
# ##########################
# ##########################
def find_candidate_itemids(_verbosity=1, sc = None):

    # #################
    # Load the dictionary db
    mydb = db.dictionary_status_setup(_verbosity)
    
    # modified the code in here to work for testing
    results = mydb.start_upc_search('UPC_SEARCH')

    # Define lists for the output
    upc_results = []
    sku_results = []
    mod_results = []
    for i,ch in enumerate(results['ChID']):
        if results['UPC'][i] is not None:
            upc_results.append(results['UPC'][i])
        if results['SKU'][i] is not None:
            sku_results.append(str(results['POSOUTLET'][i])+'NPD'+str(results['SKU'][i]))
        if results['MOD'][i] is not None and results['BRAND'][i] is not None:
            mod_results.append(str(results['BRAND'][i])+'NPD'+str(results['MOD'][i]))

    # ##########################
    # ##########################
    # Load the reference datas
    # ##########################
    # ##########################

    #conf = SparkConf()
   #sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # ##########################
    # Load the business map into the session
    busMapFile = sc.textFile('/npd/ODS/ODS_INPUTS/ODS_BUSINESSES')
    busMapData = busMapFile.collect()

    bus_map = {}
    for d in busMapData:
        d = str(d.rstrip().lstrip())
        if d == '':
            continue
        d = d.split('|')
        bus_map[int(d[1])] = d[0]


    sqlContext = SQLContext(sc)

    # ##########################
    # UPC SEARCHING
    upc_results.append(681605)
    upc_results.append(1649404792)
    upc_results.append(8810410)
    df_upc = process_searching(sqlContext,'UPC',upc_results)
    upc_list = df_upc.select("ITEMID").rdd.map(lambda x: x.ITEMID).collect()

    print(upc_list)
    
    return(upc_list)
    # ##########################
    # SKU SEARCHING
    sku_results.append('37553NPD98490106')
    df_sku = process_searching(sqlContext,'SKU',sku_results)

    # ##########################
    # MODEL number SEARCHING
    mod_results.append('CQ BELTS & HOSESNPD22951')
    df_mod = process_searching(sqlContext,'MOD',mod_results)

    
def process_searching(_sqlContext, _type,_results):
    print('Processing '+str(_type)+' Search')
    root_directory = '/npd/maps/dictionary/itemid_maps/'
    print(_results)

    data_key = 'UPC'
    if 'UPC' in _type:
        root_directory = root_directory+'upc/UPC_' 
    if 'SKU' in _type:
        root_directory = root_directory+'sku/SKU_'
        data_key = 'OUTLET_SKU'
    elif 'MOD' in _type:
        root_directory = root_directory+'model/MOD_'
        data_key = 'BRAND_MODEL'

    df = _sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "false").option("delimiter",'|').load(root_directory+'*')
    cols = []
    cols.append('_c0 as '+data_key)
    cols.append('_c1 as ITEMID')
    cols.append('_c2 as COUNT')
    cols.append('_c3 as TOTAL')
    df = df.selectExpr(cols)
    df = df.withColumn('Business', input_file_name())
    df = df.withColumn('Business', regexp_replace('Business', 'hdfs://NPDHDPSL'+root_directory, ''))
    df = df.withColumn('Business', regexp_replace('Business', '.out', ''))

    df = df.filter(col(data_key).isin(_results) == True)
    df.show()

    return(df)

#find_candidate_itemids()
