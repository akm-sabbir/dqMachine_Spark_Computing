#!/usr/bin/env python
#! -*-coding-*- utf-8
import os
# import the module that need to be tested
# use reflection technique for testing in python environment
import sys
from optparse import OptionParser
addedVar = "/"
integer = 0
#print ROOT_DIR
def combineToPath(param1 = None):
    global addedVar
    global integer
    addedVar = os.path.join(addedVar,param1)
    #print addedVar
    #sys.path.append(addedVar)
    sys.path.insert(integer,addedVar)
    integer += 1
    return

def zipdir(path):
    global integer
    for root,dirs,files in os.walk(path):
            for filing in files:
                sys.path.insert(integer,os.path.join(root,filing))
                integer += 1
def importModules(moduleName = None,func = 'm'):

    if moduleName  == None:
        print "None module can not be imported"
        return -1
    components = moduleName.split(".")
    zipdir(components[0])
    map(combineToPath, components)
    sys.path.append('../../')
    #from src.main.python.db.dictstatus_db import dictstatus_db
    #print moduleName
    replacem_string = ".".join(components[0:len(components) - 1])
    print replacem_string
    #mod =  __import__(".".join(components[0:len(components)-1]),fromlist = [components[len(components) - 1 ]])#,fromlist = ["dictstatus_db"])
    print components[len(components)-1]
    mod2 = __import__(".".join(components[ 0 : len(components) - 1]),fromlist = [components[len(components) - 1]])
    #for comp in components[1:]:
        #print type(mod)
    #   if(hasattr(mod,comp.strip()) == True):
    #      print type(mod)
    #      mod = getattr(mod,comp)
    if(hasattr(mod2,components[len(components) - 1]) == True and func == 'm'):
        myclass = getattr(mod2, components[len(components) - 1])()
        return myclass
    elif (func == 'n'):
        return mod()
    else:
        raise AttributeError
    

def main():
    module = "src.main.python"
    parser = OptionParser()
    parser.add_option("--file","-f", action = "store", dest = "fileName", type = "string")
    parser.add_option("-m","--module",action = "store", dest = "moduleName", type = "string")
    opts , args = parser.parse_args()
    if(opts.moduleName != None):
      if(opts.moduleName.find(module) == -1):
        print "incorrect module name"
        return
    '''
    if(opts.fileName != None):
        if(opts.fileName.find(".py") == -1):
            print "provide a python file for testing"
            return'''

    #if(fileName.find("testFile") == -1):
     #   fileName = "testFile." + fileName

    #with open(fileName,"r+") as dataSource
    if(opts.moduleName != None and opts.fileName == None):
        print "I am working here"
        myClass = importModules(opts.moduleName, 'm' )
        getattr(myClass,"methods")()
    else:
        myClass = importModules(opts.fileName,'n')
        getattr(myClass,"testingMain")()
    #Object = getattr(Object,"dictstatus_db")
    #Object.methods()
    return

if __name__ == "__main__":
    main()
