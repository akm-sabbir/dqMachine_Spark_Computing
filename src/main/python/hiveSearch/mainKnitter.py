#!/usr/bin/env python
#!-*- coding:utf-8 -*-
from pysparkHiveContext import hiveContextOps
from upcModule.find_candidate_itemids import find_candidate_itemids 
import time
def mainOps():
    hiveContext, sc_ = hiveContextOps.main()
    listOfItemids = find_candidate_itemids(sc = sc_)
    start_time = time.time()
    listing = 1
    if(listing is 0):
        print "Choosen option is batch processing "
    else:
        print  "Choosen option is iterative for loop"

    finalR = hiveContext.startDictSearch(listOfItemids,listing = listing)
    print "hive search time: " + str(time.time() - start_time)
    for each in finalR:
        print "total records: " + str(len(each))
        #for subeach in each:
         #   print subeach 
    return

if __name__ == "__main__":
    mainOps()


