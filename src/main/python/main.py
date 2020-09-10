#!/usr/bin/env python
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import os
import logging
import sys
import sys
#sys.path.insert(0,"/code/dqMachine/src/main/python/db")
#import db.jobstatus_db
#from src.main.python.db.jobstatus_db import jobstatus_db as js_db
#from src.main.python.preprocessor.preprocessor_bulk import preprocessor_bulk
#from preprocessor.preprocessor_bulk import preprocesssor_bulk

class Main(object):
	def __init__(self):	
		return
	def addAllpath(self,):
	    change_path = 'src/main/python/dictionary/'
	    change_path2 = './'
            change_path3 = 'test/functional/'
	    sys.path.append(os.path.abspath(change_path + "writeBackModule/checkMdoule/"))
	    sys.path.append((change_path2 + 'fileSystem/'))
	    sys.path.append(os.path.abspath(change_path2  + 'configuration/'))
	    sys.path.append(os.path.abspath(change_path2  + 'loggingMod/'))
	    sys.path.append(os.path.abspath(change_path2 +  'configurationMod/'))
	    sys.path.append(os.path.abspath(change_path2 + 'createScript/'))
	    sys.path.append(os.path.abspath(change_path2 + 'debugModule/'))
	    sys.path.append(os.path.abspath(change_path2 + 'duplicateDetector/'))
	    sys.path.append(os.path.abspath(change_path2 + 'hiveSearch/'))
	    sys.path.append(os.path.abspath(change_path2 + 'mapGeneration/'))
	    sys.path.append(os.path.abspath(change_path2 + 'upcModule/'))
	    sys.path.append(os.path.abspath(change_path2 + 'partitionCreator/'))
	    sys.path.append(os.path.abspath(change_path2 + 'itemidMetadata/'))
	    sys.path.append(os.path.abspath(change_path2 + 'fileSource/'))
	    sys.path.append(os.path.abspath(change_path2 + 'changedStatistics/'))
	    sys.path.append(os.path.abspath(change_path2 + 'writebackModule/'))
	    sys.path.append(os.path.abspath(change_path + 'fileSystem/'))
	    sys.path.append(os.path.abspath(change_path + 'changedStatistics/'))
	    sys.path.append(os.path.abspath(change_path + 'configuration/'))
	    sys.path.append(os.path.abspath(change_path +  '/configurationMod/'))
	    sys.path.append(os.path.abspath(change_path + 'createScript/'))
	    sys.path.append(os.path.abspath(change_path + 'debugModule/'))
	    sys.path.append(os.path.abspath(change_path + 'findNewAdd/'))
	    sys.path.append(os.path.abspath(change_path + 'configurationMod/'))
	    sys.path.append(os.path.abspath(change_path + 'dynamicPartitionresize/'))
	    sys.path.append(os.path.abspath(change_path + 'duplicateDetector/'))
            sys.path.append(os.path.abspath(change_path + 'writeBackModule/'))
	    sys.path.append(change_path + 'hiveSearch/')
	    sys.path.append(os.path.abspath(change_path  + 'loggingMod/'))
	    sys.path.append(os.path.abspath(change_path + 'mapGeneration/'))
	    sys.path.append(os.path.abspath(change_path + 'upcModule/'))
	    sys.path.append(os.path.abspath(change_path + 'partitionCreator/'))
	    sys.path.append(os.path.abspath(change_path + 'fileSource/'))
	    sys.path.append(os.path.abspath(change_path + 'itemidMetadata/'))
            sys.path.append(os.path.abspath(change_path))
            sys.path.append(os.path.abspath(change_path2))
	    sys.path.append(os.path.abspath(change_path3))
	    return
	def systemInfo(self,):
	    print "current working directory is :" + str(os.getcwd())
	    os_name,machine_name,os_version,access_time,os_architecture = os.uname()
	    print ("Operating system name : " + os_name)
            print ("Machine Name : " + machine_name)
	    print ("operating system version : " + os_version)
	    print ("last access time " +  access_time)
	    print ("operating system architecture : " +  os_architecture)
	    return
        def executor(self,):
            from dictionary.mainKnitter import mainOps
	    #from readandredistribute import mainOps
	    path_tracker = 'src/main/python/dictionary/fileSource/tracker'
            mainOps()
	    return
	def Print(self,):
	    print("End of the successfull operation")
	    return

if __name__ == "__main__":

	print  "main function for this pyspark project is going to be initiated soon ....."
	mainOb = Main()
	mainOb.systemInfo()
	mainOb.addAllpath()
        mainOb.executor()
        #mainOb = js_db()
        #mainOb.set_hostname('lslhdpcd1')
        #mainOb.set_database('PACEDQ')
        #mainOb.set_table('JOB_STATUS')
        #mainOb.set_primary_key_name('JOB_ID')
        #mainOb.get_setup()
        mainOb.Print()
        #preprocessor_bulk()


        
