#!usr/bin/env python

from pyspark import SparkConf, SparkContext
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
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )


def main():
    sc = init()
    quiet_logs(sc)
    data = sc.parallelize([[i for i in xrange(1,100)] for j in xrange(1,10) ]).flatMap(lambda x : int(x)*int(x)).filter(lambda x : filterFunc(x,2) ).collect()
    print "end of process execution"
    print "final output " + str(data)
    return


if __name__=='__main__':
    main()
