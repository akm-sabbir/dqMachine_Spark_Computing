import logging
from logging.handlers import SysLogHandler
import time
import sys
#from hdfsFileDiscovery import deletePath
#from hdfsFileDiscovery import getLastfilenumber 
from service import find_syslog, Service
from hdfs3 import HDFileSystem
import subprocess

class MyService(Service):
    def __init__(self, *args, **kwargs):
        super(MyService, self).__init__(*args, **kwargs)
        self.logger.addHandler(SysLogHandler(address=find_syslog(),
                               facility=SysLogHandler.LOG_DAEMON))
        self.logger.setLevel(logging.INFO)
	self.waitTime = 600
	self.argList = ["hdfs","dfs", "-ls" ,"-t" ,"-r"]
	self.odsposoutlet = self.argList + ['/npd/ODS/ODS_INPUTS_BZ2/ODS_POSOUTLETITEMS/']
	self.odsitems = self.argList + ['/npd/ODS/ODS_INPUTS_BZ2/ODS_POSITEMS']
	self.application_command  = ["spark-submit", "--driver-java-options", '"-Dlog4j.configuration=file:src/main/python/dictionary/configuration/log4j.properties"', "--master", "yarn", "--conf", "spark.driver.memory=10g", "--num-executors", "5", "--executor-memory", "8g", "--executor-cores", "4", "--py-files", "dq_machine.zip" , "--packages",  "mysql:mysql-connector-java:5.1.39", "src/main/python/main.py", "0"]
    def run_cmd(self, args_list): 
	print('Running system command: {0}'.format(' '.join(args_list))) 
	proc = subprocess.Popen(args_list, stdout = subprocess.PIPE)#, stderr = subprocess.PIPE) 
	s_output, _ = proc.communicate() 
	s_return = proc.returncode 
	return s_output
    def checkForfiles(self,):
	try:
		print "starting the application service"
		self.run_cmd(['spark-shell', '-i', "/hdpdata/pysparkProject/dqMachine/src/main/python/dictionary/service/applicationService.scala"])
		print "end of the application service"
	except:
		print "ignoring the exception"
		pass
		#s_r,s_o,s_e = run_cmd(['hdfs', 'dfs', '-copyFromLocal', 'src/main/python/dictionary/maps/itemIdWithPartition06.txt', "hdfs:////npd/s_test2/itemidMapper/"])
        #print str(s_r) +str(' and ')+ str(s_o) +' and '+ str(s_e)
	return

    def getLastfilenumber(self,filenames = None,ft = 0 , rw = 0, file_num_ = None):
	last_file_number  = None
	root_part = '/hdpdata/pysparkProject/dqMachine/'
	parentPath = root_part + 'src/main/python/dictionary/configuration/lastfilenumber' if ft is 1 else root_part + 'src/main/python/dictionary/configuration/lastfilenumberOds'
	needToread = []
	odsposoutlet = ["hdfs","dfs", "-ls" ,"-t" ,"-r", "/npd/s_test2"]
	if rw is 0:
		with open( parentPath ) as filereader:
	        	last_file_number = filereader.readline()
	   	filenames = sorted(filenames,reverse =True)
	   #needToread = []
          	print( filenames)
	   	for each in filenames:
	      		if (each.split('/')[-1] > last_file_number):
	         		needToread.append(each)
	   	new_file_number = filenames[-1].split("/")[-1]
	   	if new_file_number > last_file_number:
			return 1
	   	else:
			return 0
        else:
	   	with open( parentPath ,'w+') as datawriter:
              		if file_num_ is not None:
		 		print "last file name: " + file_num_
	        datawriter.write(str(file_num_))
	   	return None, None
		
	return file_number
    def process_output(self, data_list):
	items = data_list.split("\n")[1:]
	for ind, each in enumerate(items):
		items[ind] = each.split(" ")[-1].split("/")[-1]
	items = sorted(items)
	return items[-1]
    def run(self):
	fileTypes = [0 , 1]
	root_part = '/hdpdata/pysparkProject/dqMachine/'
        common_part = 'src/main/python/dictionary/configuration/'
	file_paths = [ root_part + common_part + 'lastfilenumber', root_part + common_part + 'lastfilenumberOds']
	#hdfs = HDFileSystem(host=,port=)
	data_writer = open(root_part + 'src/main/python/dictionary/service/test_output','a+')
	try:
            while not self.got_sigterm():
	    	apps_tracking  = [ root_part + common_part + 'tracker']
	    	#self.checkForfiles()
	    	time.sleep(10)
 		response_list = [0 , 0]
		for ind,each in enumerate(file_paths):
			with open(each, 'r+') as datareader:
				response_list[ind] = datareader.readlines()
		datum = None
		print response_list
		outputPos = self.run_cmd(self.odsposoutlet)
		outputItem = self.run_cmd(self.odsitems)
		lastoutputPos = self.process_output(outputPos)
		lastoutputItem = self.process_output(outputItem)
		if lastoutputPos > response_list[0] and lastoutputItem > response_list[1] :
			data_writer.write("we have to wait till all files are written")
			time.sleep(20)# self.waitTime
			data_writer.write("all files are available now start application for data preprocessing")
			self.run_cmd(self.application_command)
		else:
			data_writer.write("we can go back to sleep, no need to initiate the programe")
		try:
				#self.run_cmd(['spark-shell', '-i', "/hdpdata/pysparkProject/dqMachine/src/main/python/dictionary/service/applicationService.scala"])
			data_writer.write( "I am going to sleep now" + '\n')
			time.sleep(10)
		except:
			data_writer.write( "ignoring the exception" + '\n')
			pass
	finally:
		print "Break out signal recieved "
		data_writer.close()

	return

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        sys.exit('Syntax: %s COMMAND' % sys.argv[0])

    cmd = sys.argv[1].lower()
    service = MyService('my_service', pid_dir='/tmp')
    if cmd == 'start':
	print "service is going to start"
        service.start()
    elif cmd == 'stop':
	print "we are trying to stop the service"
        service.stop()
    elif cmd == 'status':
        if service.is_running():
            print "Service is running."
        else:
            print "Service is not running."
    else:
        sys.exit('Unknown command "%s".' % cmd)
   
