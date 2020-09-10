#!/usr/bin/env python

import os
import sys

def reasonBehindExit(msg):
	print "Application is going to exit"
	print(msg)
	print " return"
	return 1
def reasonOfException(msg, handler = None):
	if handler is not None:
		handler.info(msg)
	else:
		print "There is exception occured"
		print(msg)
		print "return"
	return 2
def applicationDebugModule(msg = None, _type = 0):
	switching = {

		0:reasonBehindExit(msg),
		1:reasonOfException(msg) 
	}
	return  switching.get(_type,None)
