#!usr/bin/env python

from pyspark import SparkConf, SparkContext
#import org.apache.hadoop.fs.FileSystem
import logging
def filterFunc(x, arg1):
    if x % arg1 == 1:
        return 1
    else:
        return 0
    return
def init(appName = "defaultApp",machineMaster="local[4]"):
    config = SparkConf().setAppName(appName).setMaster(machineMaster)
    sc = SparkContext(conf = config)
    return sc
    #rootLogger = logging.getLogger('py4j')
    #rootLogger.setLevel(Level.ERROR)
    #logging.getLogger("org").setLevel(Level.OFF)
    #loggin.getLogger("akka").setLevel(Level.OFF)

def quiet_logs( sc ):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel( logger.Level.INFO )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.INFO )
    logger.appender.RollingAppenderU=logger.DailyRollingFileAppender
    logger.appender.RollingAppenderU.File="/home/abdul.sabbir/sparkU.log"
    logger.appender.RollingAppenderU.DatePattern='.'"yyyy-MM-dd"
    logger.appender.RollingAppenderU.layout=logger.PatternLayout
    logger.appender.RollingAppenderU.layout.ConversionPattern="[%p] %d %c %M - %m%n"
    logger.logger.myLogger = logger.INFO,logger.appender.RollingAppenderU
    logger.logger.spark.storage = logging.INFO, "/home/abdul.sabbir/RollingAppender"
    logger.additivity.spark.storage = False
    logger.logger.spark.scheduler = logging.INFO, "/home/abdul.sabbir/RollingAppender"
    logger.additivity.spark.scheduler = False
    logger.logger.spark.CacheTracker=logging.INFO,"/home/abdul.sabbir/RollingAppender"
    logger.additivity.spark.CacheTracker=False
    logger.logger.spark.CacheTrackerActor=logging.INFO,"/home/abdul.sabbir/RollingAppender"
    logger.additivity.spark.CacheTrackerActor=False
    logger.logger.spark.MapOutputTrackerActor=logging.INFO,"/home/abdul.sabbir/RollingAppender"
    logger.additivity.spark.MapOutputTrackerActor=False
    logger.logger.spark.MapOutputTracker=logging.INFO, "/home/abdul.sabbir/RollingAppender"
    logger.additivty.spark.MapOutputTracker=False
    logger.logger.spark.LogManager = logging.INFO,"RollingAppender"
    return logger

def rddOps():
    sc = init()
    inputPath = ""#sys.argv[1]
    outputPath = "/npd/processed/sabbirDir "#sys.argv[2]
    # this is for quieting the log information 
    logger = quiet_logs(sc)
    log = logger.LogManager.getRootLogger()
    log.setLevel(logger.Level.INFO)
    log.info("this is a warning message")
    bigdataRdd = sc.textFile("hdfs:/npd/ODS/ODS_INPUTS/ODS_CODE_ITEMNUMBERS/part-m-00000.gz")
    rdd = bigdataRdd.map(lambda x: x.split("|")).take(25)
    for each in rdd:
        print each
    # perform most of the work in functional way
    data = sc.parallelize([[i for i in xrange(1,100)] for j in xrange(1,10) ]).flatMap(lambda y : map(lambda x : x * x,y)).filter(lambda x : filterFunc(x,2) )
    log.info("I am done with the warning message")
    print data.toDebugString()
    data = data.collect()
    data.saveAsTextFile(outputPath)
    print "end of process execution"
    #print "final output " + str(data)
    return


#if __name__=='__main__':
#  main()
