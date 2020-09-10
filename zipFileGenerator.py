#!/usr/bin/env python
import os
import zipfile
import sys
from subprocess import call
def zipdir(path,ziph):
    for root,dirs,files in os.walk(path):
        for filing in files:
            if(filing.find(".py") == -1):
                continue
            print str(os.path.join(root,filing) + " added to archive")
            ziph.write(os.path.join(root,filing))
    return

if __name__ == '__main__':
        if(len(sys.argv) >= 2 and  sys.argv[1] == 'cleans'):
            print("performing cleaning")
            call(["rm", "dq_machine.zip"])
        elif (len(sys.argv) >= 2 and (sys.argv[1] == "test" or sys.argv[1] == "build")):
            print("generating zipfile: ")
            zipf = zipfile.ZipFile('dq_machine.zip','w',zipfile.ZIP_DEFLATED)
            if(sys.argv[1] == "build"):
                zipdir('src',zipf)
            else:
                zipdir('test',zipf)
            zipf.close()
