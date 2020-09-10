#!/usr/bin/env
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
from pyspark import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import avg, udf, col
from pyspark.sql.types import StringType
import MySQLdb
import numpy as np
from collections import defaultdict
#from find_candidate_itemids import find_candidate_itemids
'''
try:
    from Tkinter import *
except ImportError:
    from tkinter import *

import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
'''
'''Author by AKM Sabbir'''


class hiveContextOps():
    
    def __init__(self,configFile="itemIdWithPartition.txt"):
        self.configFile = configFile
        self.itemIdDict = defaultdict(str)
        return
    def testData(self, fileName = "itemidOutput.txt", sep = '\t'):
        with open( fileName , "r+" ) as dataSource:
            for ind, each in enumerate(dataSource):
                yield (ind, each.split(sep))
        return
    def hivePropertySetup(self,):
        self.hive_context.sql('set mapreduce.map.memory.mb=6000')
        self.hive_context.sql('set mapreduce.map.java.opts=-Xmx9000m')   
        self.hive_context.sql('set mapreduce.reduce.memory.mb=9000')
        self.hive.context.sql('set mapreduce.reduce.java.opts=-Xmx7200m')
        self.hive_context.sql('set hive.exec.dynamic.partition.mode=nonstrict')
        self.hive_context.sql('set hive.tez.container.size=10240')
        self.hive.context.sql('set hive.tez.java.opts=-Xmx8192m')
        return
    def generateStat(self,):
        totalList = []
        for each in self.testData():
            totalList.append(each)
        totalList = sorted(totalList, key = lambda x : int(x[1][1].strip()))
        print str(totalList[0]) + " " + str(totalList[len(totalList) - 1])
        with open("sortedItems.txt", "w+") as dataWriter:
            for each in totalList:
                each_1 = each[0].strip()if(isinstance(each[0],int) == False) else each[0]
                each_2 =  each[1][0].strip() if(isinstance(each[1][0],int) == False) else  each[1][0]
                each_3 = each[1][1].strip() if(isinstance(each[1][1], int) == False) else each[1][1]
                dataWriter.write(str(each_1) + '\t' + str(each_2) + '\t' + str(each_3) + '\n')
        return
    def secondTest(self, ):
        dict_ = defaultdict(list)
        for each in self.testData(fileName = "sortedItems.txt"):
            dict_[int(each[1][2].strip())].append(long(each[1][1].strip()))
        result = []
        for key, value in dict_.iteritems():
                result.append((key,len(value)))
        result = sorted(result, key = lambda x : x[1])
        with open("final_result","w+") as dataWriter:
            for each in result : 
                dataWriter.write(str(each[0]) + " " + str(each[1]) + '\n')
            
        return
    def combinePartitionWithItemid(self):
        dict_ = defaultdict(list)
        for each in self.testData(fileName = "sortedItems.txt"):
            dict_[int(each[1][2].strip())].append(long(each[1][1].strip()))
        dict_2 = defaultdict(str)
        with open("final_result2", "r+") as data_reader2:
            data = data_reader2.readlines()           
            for datum in data:
                datum_ = datum.split('\t')
                if(dict_2[long(datum_[0].strip())] == ''):
                    dict_2[long(datum_[0].strip())] = datum_[1].strip()
        with open("itemIdWithPartition.txt","w+") as data_writer:
            for key,val in dict_.iteritems():
                for each in val:
                    data_writer.write(str(key) + "\t"+ str(each) + "\t" + str(dict_2[key].strip()) + "\n" )

        return

    def startMergeAndSort(self, numbers):
        if(len(numbers) > 500):
            i = len(numbers)-1
            j = 0
            results = []
            while(True):
                if(i < j):
                    break
                results.append((numbers[i][0]+numbers[j][0], numbers[i][1] + numbers[j][1]))
                i-=1
                j+=1
            results = sorted(results, key = lambda x : x[1])
            numbers = self.startMergeAndSort(results)
        return numbers

    def getStats(self,):
        numbers = []
        for each in self.testData(fileName="final_result",sep=' '):
            numbers.append(([each[1][0]],float(each[1][0])*float(each[1][1].strip())))
        numbers = sorted(numbers, key = lambda x : x[1])
        data = self.startMergeAndSort(numbers)
        with open("final_result2", "w+") as data_writer2:
            for ind,each in enumerate(data):
                for datum in each[0]:
                    data_writer2.write(str(datum) + "\t" + "partition_" + str(ind) + "\t" + str(each[1])+'\n')
            
        #with open("final_result", "w+") as data_writer:
         #   data_writer.write()
        #print str(np.mean(numbers)) + " " + str(np.std(numbers))
        return

    def quiet_logs(self,sc_ = None):
        logger = sc_._jvm.org.apache.log4j
        logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
        logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
        return

    def init(self):
        config = SparkConf().setAppName("defaultName").setMaster("local[8]")
        sc = SparkContext(conf = config)
        return sc

    def createContext(self, sc_ = None):
        self.hive_context = HiveContext(sc_)
        self.sqlContext = SQLContext(sc_)
        return

    def selectDatabase(self, dbName):
        self.hive_context.sql("use " + dbName)
        return

    def setConditionCol(self,colName):
        self.colName = colName
        return

    def initItemIds(self,):
        try:
            with open(self.configFile, "r+") as dataReader:
                for each in dataReader:
                    data = each.split('\t')
                    self.itemIdDict[long(data[1].strip())] = data[2].strip()
        except FileNotFoundError as e:
            print (e)
            return None
        except IndexError as e:
            print (e)
            return None
        

    def retrievePartition(self,itemId ):
        
        try:
            if(self.itemIdDict[itemId] == ''):
                return None
            else:
                return self.itemIdDict[itemId].strip()
        except KeyError as e:
            print "exception key Error"
            return ("error",e)

    def getSearchResult(self,partition = None,listOfItemid = None,listing = 0):
        if(listOfItemid ==  None):
            print "list is null so no new Add"
            raise ValueError
            return
        if(listing is 0):
            for ind, each in enumerate(partition):
                partition[ind] = "'"+each+"'"
            listOfItemid = [str(each) for each in listOfItemid]
            searchString = "select poi_id, itemid, business_id, outletdescription from " + self.setTableName + " where array_contains(array(" + ','.join(listOfItemid) + "), itemid   )  and array_contains(array(" + ','.join(partition) + "),partitioner)"
        else:
            searchString = "select poi_id, itemid, business_id, outletdescription from " + self.setTableName + " where  partitioner='" +  partition.strip() + "' and itemid=" + str(listOfItemid)
        #print searchString        
        try:
            result = self.hive_context.sql(searchString)
        except (MySQLdb.Error, MySQLdb.Warning ) as e :
            print("exception MySQlDB: ")
            print (e)
            return None
        except TypeError as e:
            print ("exception TypeError : ")
            print (e)
            return None
        except ValueError as e:
            print ('something wrong with columns or conditions')
            print (e)
            return None
        finally:
            pass

        result = result.rdd.map( lambda x : (x.itemid, x.poi_id,x.business_id,x.outletdescription))
        return result.collect()
        
        self.sqlContext.createDataFrame(result).createOrReplaceTempView(self.setTableName)
        results = self.sqlContext.sql("select itemid, businessid from " + self.setTableName   )
        #results = results.rdd.map( lambda x : x.itemid + " " + x.businessid).take(50)
        '''
        schemaString = "tableName"
        fields = [StructField(f_name, StringType()) for f_name in schemaString.split()]
        schema = StructType(fields)
        schemaTab = sqlContext.createDataFrame(result,schema)
        schemaTab.show()
        '''
        #bank = self.hive_context.table('odsitems')
        #bank.show()
    def searchTable(self, tableName):
        if(tableName == None):
            print "provide a valid table Name"
            return
        self.setTableName = tableName

        return
    def selectDatabase(self,dbName ):
        if(dbName == None):
            print "provide database name"
            return
        try:
            self.hive_context.sql("use " + dbName)
        except:
            return 0
        return 1
    def startDictSearch(self,listOfItems = None,listing = 0):
        final_result = []
        if(listOfItems == None):
            return None
        if(len(self.itemIdDict) == 0):
            self.initItemId()
    
        if(listing is 1):
            for each in listOfItems:
                partition = self.retrievePartition(each)
                if(partition == None):
                    print("the itemid is missing in dictionary") 
                    continue
                elif (isinstance(partition,tuple()) == True):
                    if(partition[0] == "error"):
                        print(e) 
                else:
                    final_result.append(self.getSearchResult(partition=partition, listOfItemid = each,listing = listing))
        else:
            listOfPart = []
            for each in listOfItems:
                listOfPart.append(self.retrievePartition(long(each))) 
            final_result.append(self.getSearchResult(partition = listOfPart, listOfItemid = listOfItems,listing = listing))
        #except TypeError as e:
        #   print "exception typeError : "
        #    print e
        #    return 
        return final_result

    @staticmethod
    def main():
        this = hiveContextOps()
        sc_ = this.init()
        this.createContext(sc_)
        this.selectDatabase('karun_test')
        this.searchTable('karun_test.odsposoutletitemswithpartition')
        this.quiet_logs(sc_)
        this.initItemIds()
        return this, sc_
# main sequence of operation 
# initally create the hivecontextObject then call init function, then quiet the log and creat the hive and scala context to call
# appropirate function 
#def execute(listOfItemid == None):
 #   if(listOfItemid == None):
 #       print "need to provide the required itemids"
  #      raise ValueError
  #      return
def generatingUniformDistributionOfData(this):
    this.generateStat()
    this.secondTest()
    this.getStats()
    this.comginePartitionWithItemid()
    return

#if __name__== '__main__':
    
#   this, sc_  = hiveContextOps.main()
#   this.startDictSearch(  listOfItems=[1224712],listing=1)
    '''
    generateingUniformDistributionOfData(this)
    sc = this.init()
    this.quiet_logs(sc)
    this.createContext(sc)
    this.selectDatabase('karun_test')
    this.searchTable('odsitems')
    this.setConditionCol('businessid')
    this.getSearchResult(listOfItemid = [53,61])
    '''
