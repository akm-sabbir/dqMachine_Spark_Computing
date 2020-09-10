
############################
############################
## 
## Job Status controller
## 
############################
############################

#from collections import Counter,defaultdict,namedtuple
import MySQLdb

import subprocess
from datetime import datetime
import os

class jobstatus_db(object):
    """
       Job Status Controller
       """

    # ##################################
    # Define the object
    def __init__(self):
        ## Here will be the logs information
        self.hostname = 'localhost'
        self.db_name = ''
        self.JobID_name = ''
        self.table_name = ''
        self.db_JobID = -1
        self.db_JobIsNew = True
        self.db_GroupID = -1
        self.cursor = None
   # ##################################
    def __str__(self):
        return "this is for mysql Job Status info"

    # ###################################
    #  Add a method "length" to the object
    def table_stats(self):
        print('table_stats')
        return('table_stats')
    # ###################################

    # ###################################
    # Set hostname
    def set_hostname(self, _hn):
        self.hostname = _hn
    # ###################################

    # ###################################
    # Set DB
    def set_database(self, _db):
        self.db_name = _db
    # ###################################

    # ###################################
    # Set Primary Key name (JobID
    def set_primary_key_name(self, _name):
        self.JobID_name = _name
    # ###################################

    # ###################################
    # Set Table
    def set_table(self, _table):
        self.table_name = _table
    # ###################################


    # ###################################
    # Get Primary_id
    def get_primary_id(self):
        return(self.db_JobID)
    # ###################################
    def get_cursor(self):
        if self.cursor == None : 
            self.cursor = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name).cursor()
        return self.cursor

    # ###################################
    #  Set the job id based on the input
    # NEED TO IMPLEMENT THE _FILETYPE CHECK
    def set_jobid(self,_retailer,_year,_month,_week,_load_id,_filetype='fgo',_quiet=False):
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor()

        columns = ['RETAILER_ID','YEAR','MONTH','WEEK','LOAD_ID']
        values  = [_retailer,_year,_month,_week,str(_load_id)]

        sql = 'SELECT '+self.JobID_name+' FROM '+self.table_name+' WHERE '
        for i,column in enumerate(columns):
            if column == 'Filepath':
                continue
            if i > 0:
                sql = sql+' AND '
            sql = sql + column + ' = ' + str(values[i])

        job_id = self.test_exists(cursor, sql)
        if job_id >= 0:
            if _quiet == False:
                print('This already exists: '+str(job_id))
            self.db_JobID = job_id
            self.db_JobIsNew = False
            db.close()
            return(job_id)

        db.close()
        print('ooops, no job id was found!')
        return(-1)



    # ###################################
    #  Set methods for each items
    def start_new_jobid(self,_filename,_status='STARTED'):
        filename = _filename.split('/')[-1]
        retailer = filename[1:5]
        year     = filename[5:9]
        month    = filename[9:11]
        week     = filename[11:12]
        load_id  = 0
        try: 
            # From the file name
            load_id = int(_filename.split('.')[-3])
        except:
            try:
                # From the directory structure
                load_id = int(_filename.split('/')[-2])
            except:
                # I don't know, set to 0
                load_id = 0
        # Catch when the retailer number populates the load_id field, set it to zero
        if int(load_id) == int(retailer):
            load_id = 0

        columns = ['RETAILER_ID','YEAR','MONTH','WEEK','LOAD_ID','FILEPATH']
        values  = [retailer,year,month,week,str(load_id),'"'+_filename+'"']

        sql = 'SELECT '+self.JobID_name+' FROM '+self.table_name+' WHERE '
        for i,column in enumerate(columns):
            if column == 'FILEPATH':
                continue
            if i > 0:
                sql = sql+' AND '
            sql = sql+column+' = '+str(values[i])

        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor() #db.cursor()

        job_id = self.test_exists(cursor, sql)
        if job_id >= 0:
            print('This already exists: '+str(job_id))
            self.db_JobID = job_id
            self.db_JobIsNew = False
            db.close()
            return(job_id)
        self.db_JobIsNew = True        

        columns.append('DISCOVERY_START_TIMESTAMP')
        columns.append('DISCOVERY_STATUS')
        columns.append('FILE_TYPE')
        #values.append(self.get_current_time())
        insert_sql = 'INSERT INTO '+self.table_name+'('+','.join(columns)+')'
        insert_sql = insert_sql+' VALUES('+','.join(values)+',CURRENT_TIMESTAMP,"'+_status+'","'+filename[0]+'")'
        
        cursor.execute(insert_sql)
        db.commit()
        self.db_JobID = self.test_exists(cursor, sql)
        self.update(db,cursor,'DISCOVERY_STATUS','"'+_status+'"')

        # Set all the Statii for process components to NOT_STARTED
        self.update(db,cursor,'PRE_PROCESSOR_STATUS','\"NOT_STARTED\"')
        self.update(db,cursor,'ANALYTICS_STATUS','\"NOT_STARTED\"')
        self.update(db,cursor,'ELASTICSEARCH_STATUS','\"NOT_STARTED\"')
        self.update(db,cursor,'ANALYST_STATUS','\"NOT_STARTED\"')
        self.update(db,cursor,'DISCOVERY_TRANSFER_TIME','0')
        self.update(db,cursor,'DISCOVERY_SPLIT_TIME','0')
        self.update(db,cursor,'DISCOVERY_SPLIT_SEGMENTS','1')

        db.close()
        return(self.db_JobID)

    # #######################
    # For starting the reva_summary table (for example reva_summary)
    def start_jobid(self,_jobid_name, _jobid):
        sql = 'SELECT '+self.JobID_name+' FROM '+self.table_name+' WHERE '+_jobid_name+'='+str(_jobid)

        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor() #db.cursor()

        job_id = self.test_exists(cursor, sql)
        if job_id >= 0:
            print('This already exists: '+str(job_id))
            self.db_JobID = job_id
            self.db_JobIsNew = False
            db.close()
            return(job_id)
        self.db_JobIsNew = True        

        insert_sql = 'INSERT INTO '+self.table_name+'(JOB_ID)'
        insert_sql = insert_sql+' VALUES('+str(_jobid)+')'
        
        cursor.execute(insert_sql)
        db.commit()
        self.db_JobID = self.test_exists(cursor, sql)
        db.close()
        return(self.db_JobID)

    # #######################
    # #######################
    # For starting a new db entry with input values NOT FINISHED
    # #######################
    # #######################
    def set_db(self, _names, _values, _check_cols):
        # #######################
        # Generate the SQL query, this query only selects the 'fixed' variables
        sql = 'SELECT '+self.JobID_name+' FROM '+self.table_name+' WHERE '
        for i,column in enumerate(_names):
            if i not in _check_cols:
                continue
            if i > min(_check_cols):
                sql = sql+' AND '
            sql = sql+column+' = '+str(_values[i])

        # #######################
        # Connect to the DB
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor()  #db.cursor()

        # #######################
        # test if this entry already exists
        job_id = self.test_exists(cursor, sql)

        # #######################
        # If already exists, we only update        
        if job_id >= 0:
            print('This already exists: '+str(job_id))
            self.db_JobID = job_id
            self.db_JobIsNew = False
            
            for i,column in enumerate(_names):
                self.update(db,cursor,_names[i],_values[i])

            db.close()
            return(job_id)
        self.db_JobIsNew = True

        # #######################
        # It did not exist, so we insert
        insert_sql = 'INSERT INTO '+self.table_name+'('+','.join(_names)+')'
        insert_sql = insert_sql+' VALUES('+','.join(_values)+')'
        
        cursor.execute(insert_sql)
        db.commit()
        self.db_JobID = self.test_exists(cursor, sql)
        db.close()
        return(self.db_JobID)


    # ###################################
    #  Set methods for each items
    def test_exists(self, _cursor, _sql):

        _cursor.execute(_sql)
        results = _cursor.fetchall()

        # If only one result, return that job number
        if len(results) == 1:
            return(int(results[0][0]))
        
        # If more than one, return the the last
        if len(results) > 1:
            return(int(results[-1][0]))

        return(-1)

    # ###################################
    # Get setup
    def get_setup(self):
        print('\n---------------')
        print('Database setup: ')
        print('---------------')
        print('Hostname: '+self.hostname)
        print('Database: '+self.db_name)
        print('Table:    '+self.table_name)
        print('Primary:  '+self.JobID_name)
        if self.db_JobID == -1:
            print('Last Job: None found')
        else:
            print('Last Job: '+str(self.db_JobID))
        print('---------------\n')
    # ###################################



    # ###################################
    #  Get whether this is a new job or not
    def get_is_new(self):
        return(self.db_JobIsNew)



    # ###################################
    #  Get Status
    def get_status(self, _cursor, _column_name):
        sql = 'SELECT '+_column_name+' FROM '+self.table_name+' '
        sql = sql+'WHERE '+self.JobID_name+' = '+str(self.db_JobID)+';'
        
        _cursor.execute(sql)
        results = _cursor.fetchall()
        
        return(results[0][0])

    # ###################################
    #  Get Status
    def get_status_force(self, _column_name):
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor() #db.cursor()
        
        sql = 'SELECT '+_column_name+' FROM '+self.table_name+' '
        sql = sql+'WHERE '+self.JobID_name+' = '+str(self.db_JobID)+';'

        cursor.execute(sql)
        results = cursor.fetchall()

        db.close()
        return(results[0][0])


    # ###################################
    #  Update field
    def update(self, _db, _cursor, _column_name, _value):
        sql = 'UPDATE '+self.table_name+' '
        sql = sql+'SET '+_column_name+' = '+str(_value)+' '
        sql = sql+'WHERE '+self.JobID_name+' = '+str(self.db_JobID)+';'
        
        _cursor.execute(sql)
        _db.commit()


    # ###################################
    #  Start a process
    def start_process(self, _process_name):
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor() #db.cursor()

        status = self.get_status(cursor,_process_name+'_STATUS')
        if status == 'STARTED' or status == 'COMPLETE':
            print(_process_name+' has '+status+' for job '+str(self.db_JobID))
            db.close()
            return(-1)
        elif status == 'CRASHED':
            print(_process_name+' had previously '+status+' on job '+str(self.db_JobID)+' restarting')
        
        self.update(db,cursor,_process_name+'_START_TIMESTAMP','CURRENT_TIMESTAMP')
        self.update(db,cursor,_process_name+'_STATUS','\"STARTED\"')

        db.close()
        return(1)

    # ###################################
    #  End a process
    def end_process(self, _process_name, _new_status):
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor() #db.cursor()

        status = self.get_status(cursor,_process_name+'_STATUS')
        if status is not None:
            if status != 'STARTED' and 'STARTING_' not in status:
                print(_process_name+' has '+status+' for job '+str(self.db_JobID)+' no update is made')
                db.close()
                return(-1)
        else:
            status = 'None'
        
        print(_process_name+' had previously '+status+' on job '+str(self.db_JobID)+' setting to '+_new_status)

        self.update(db,cursor,'ANALYTICS_ANOMALIES','IFNULL(CHECK_STATUS_ZIP_CHECKER,0)+IFNULL(CHECK_STATUS_DEPT_CHECKER,0)+IFNULL(CHECK_STATUS_DEPT_TRENDS,0)+IFNULL(CHECK_STATUS_STORE_TRENDS,0)+IFNULL(CHECK_STATUS_ITEM_CHECKER,0)+IFNULL(CHECK_STATUS_BRAND_CHECKER,0)')
        self.update(db,cursor,_process_name+'_END_TIMESTAMP','CURRENT_TIMESTAMP')
        self.update(db,cursor,_process_name+'_STATUS','\"'+_new_status+'\"')

        db.close()

    # ###################################
    #  Transfer Time
    def set_transfer_time(self, _transfer_time):
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor() #db.cursor()

        self.update(db,cursor,'DISCOVERY_TRANSFER_TIME',str(_transfer_time))

        db.close()

    # ###################################
    #  Anomalies Found
    def set_anomalies(self, _process_name, _process_anomaly, _number):
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor =self.get_cursor() #db.cursor()

        status = self.get_status(cursor,_process_name+'_STATUS')
        if status != 'COMPLETE':
            print(_process_name+' has '+status+' for job '+str(self.db_JobID)+' no update is made')
            db.close()
            return(-1)

        self.update(db,cursor,_process_name+'_'+_process_anomaly,str(_number))

        db.close()

    # ###################################
    #  Set a name
    def set_name(self, _process_name, _name):
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor() #db.cursor()

        status = self.get_status(cursor,_process_name+'_STATUS')
        if status != 'COMPLETE':
            print(_process_name+' has '+status+' for job '+str(self.db_JobID)+' no update is made')
            db.close()
            return(-1)

        self.update(db,cursor,_process_name+'_NAME','"'+_name+'"')

        db.close()

    # ###################################
    #  Set a status code
    def set_status_code(self, _process_name, _code_name, _code):
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor() #db.cursor()

        status = self.get_status(cursor,_process_name+'_STATUS')
        if status != 'COMPLETE':
            print(_process_name+' has '+status+' for job '+str(self.db_JobID)+' no update is made')
            db.close()
            return(-1)

        self.update(db,cursor,_process_name+'_'+_code_name,'"'+_code+'"')

        db.close()

    # ###################################
    #  Set a status code
    def set_status_code_force(self, _process_name, _code_name, _code):
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor() #db.cursor()

        status = self.get_status(cursor,_process_name+'_STATUS')

        self.update(db,cursor,_process_name+'_'+_code_name,'"'+_code+'"')

        db.close()

    # ###################################
    #  Set a free-form comment
    def set_comment(self, _process_name, _comment_name, _comment):
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor() # db.cursor()

        status = self.get_status(cursor,_process_name+'_STATUS')
        if status != 'COMPLETE':
            print(_process_name+' has '+status+' for job '+str(self.db_JobID)+' no update is made')
            db.close()
            return(-1)

        self.update(db,cursor,_process_name+'_'+_comment_name,'"'+_comment+'"')

        db.close()



    # ###################################
    #  Set a value
    def set_value(self, _name, _value):
       # db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor()  #db.cursor()

        try:
            self.update(db,cursor,_name,int(_value))
        except:
            try:
                self.update(db,cursor,_name,float(_value))
            except:
                self.update(db,cursor,_name,'"'+str(_value)+'"')

        db.close()


    # ###################################
    #  Print
    def Print(self):
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor() #db.cursor()

        sql = 'SELECT * FROM '+self.table_name
        sql = sql + ' WHERE ' + self.JobID_name +' = ' + str(self.db_JobID)

        cursor.execute(sql)
        output = cursor.fetchall()
        for row in output:
            print(row)
        
        if len(output) == 1:
            print('Only '+str(len(output))+' row is found')
        else:
            print('There are '+str(len(output))+' rows found') 
        db.close()




    # ###################################
    #  Print
    def get_latest_files(self, _retailer, _by_YMW = True, _ana_status = 'ZIP_CHECKER_COMPLETE'):
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor() #db.cursor()

        # Get all entries for this retailer
        my_query = 'JOB_ID,YEAR,MONTH,WEEK,LOAD_ID,DISCOVERY_START_TIMESTAMP,DISCOVERY_STATUS,PRE_PROCESSOR_STATUS,ANALYTICS_STATUS'
        sql = 'SELECT '+my_query+' FROM '+self.table_name
        sql = sql + ' WHERE RETAILER_ID = '+str(_retailer)

        cursor.execute(sql)
        output = cursor.fetchall()
        db.close()

        headers = {}
        for n,h in enumerate(my_query.split(',')):
            headers[h] = n
        ymw = {}
        latest_load = {}
        ana_status = {}

        for row in output:
            my_year  = row[headers['YEAR']]
            my_month = row[headers['MONTH']]
            my_week  = row[headers['WEEK']]
            my_load  = row[headers['LOAD_ID']]
            my_ymw   = 1000*my_year+10*my_month+my_week

            # Take the highest load_id only
            if my_ymw in latest_load:
                if my_load < latest_load[my_ymw]:
                    continue
                        
            if 'NOT_STARTED' in row[headers['PRE_PROCESSOR_STATUS']]:
                print(str(my_ymw)+' '+str(my_load)+' '+str(row[headers['JOB_ID']])+
                      ' has not started pre-processing, skipping this file')
                continue

            latest_load[my_ymw] = my_load
            ymw[my_ymw] = str(my_ymw)+'/'+str(my_load)+'/'
            ana_status[my_ymw] = row[headers['ANALYTICS_STATUS']]

            print(str(my_ymw)+' '+str(my_load)+' jobid:'+str(row[headers['JOB_ID']]))
            for k,v in headers.items():
                if 'STATUS' in k:
                    print('   -> '+k+': '+row[v])

        if len(ymw) == 0:
            print('No data is ready for '+str(_retailer))
            return(-1)
        
        latest_data = max(ymw.keys())
        if 'COMPLETE' in ana_status[latest_data] and _ana_status != '':
            print(str(my_ymw)+' '+str(my_load)+' '+str(row[headers['JOB_ID']])+
                  ' has already completed the zip checker, skipping this analysis')
            return(0)

        return(ymw)


    ############################
    ##
    ## Get a list of files which the history, which are waiting to be preproc'd
    ##
    def get_waiting_history(self, _retailer, _year = -1,_filetype='fg'):
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor()  #db.cursor()

        # Get all entries for this retailer
        my_query = 'JOB_ID,YEAR,MONTH,WEEK,LOAD_ID,FILE_TYPE,DISCOVERY_STATUS,PRE_PROCESSOR_STATUS,ELASTICSEARCH_STATUS'
        sql = 'SELECT '+my_query+' FROM '+self.table_name
        sql = sql + ' WHERE RETAILER_ID = '+str(_retailer)

        cursor.execute(sql)
        output = cursor.fetchall()
        db.close()

        headers = {}
        for n,h in enumerate(my_query.split(',')):
            headers[h] = n
        ymw = {}
        latest_load = {}
        _status = {}
        _type = {}

        for row in output:
            my_year  = row[headers['YEAR']]
            my_month = row[headers['MONTH']]
            my_week  = row[headers['WEEK']]
            my_load  = row[headers['LOAD_ID']]
            my_filetype = row[headers['FILE_TYPE']]
            my_ymw   = 1000*my_year+10*my_month+my_week

            # #########
            # if Augustus Caesar is the roman emperor, then we let all years through
            if _year != -1:
                if int(_year) != int(my_year):
                    continue

            # Take the highest load_id only
            if my_ymw in latest_load:
                if my_load < latest_load[my_ymw]:
                    continue

            if my_filetype in _filetype:
                print(my_filetype)
            else:
                print('Skipping this type: '+my_filetype+' not in '+_filetype)


            latest_load[my_ymw] = int(my_load)
            ymw[my_ymw] = str(my_ymw)


            if ( ('NOT_STARTED' in row[headers['PRE_PROCESSOR_STATUS']] or 'DID_NOT_TRY_TO_WRITE' in row[headers['PRE_PROCESSOR_STATUS']]) and
                 'NOT_STARTED' in row[headers['ELASTICSEARCH_STATUS']] and
                 'COMPLETE' == row[headers['DISCOVERY_STATUS']] ):
                print(str(my_ymw)+' '+str(my_load)+' '+str(row[headers['JOB_ID']])+
                      ' has not started pre-processing, adding this file')
                _status[my_ymw] = True
            else:
                _status[my_ymw] = False

            latest_load[my_ymw] = int(my_load)
            ymw[my_ymw] = str(my_ymw)

        if len(ymw) == 0:
            print('No data is ready for '+str(_retailer))
            return(-1)

        _retailer = '{:04d}'.format(int(_retailer))
        filepath = '/npd/rawdata/data/retailerinput/retailer_'+str(_retailer)+'/'+str(_retailer)
        filepath = filepath
        print(filepath+' '+str(my_load)+' jobid:'+str(row[headers['JOB_ID']]))

        print(filepath)
        all_files = []
        for k in sorted(ymw.keys()):
            if _status[k]:
                all_files.append(filepath+'/'+str(ymw[k])+'/'+str(latest_load[k]))
            else:
                 print('rejecting: '+str(ymw[k])+'/'+str(latest_load[k]))
        return(all_files)

    ############################
    ##
    ## Get the anomalies which we looked at last time
    ##
    def get_all_anomalies(self,_job_id):
        if self.table_name != 'REVA_ANOMALIES':
            print('This function is not available to '+self.table_name)
            return({})
        
        #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = self.get_cursor() #db.cursor()
        # Get all entries for this retailer
        my_query = 'JOB_ID,GROUP_ID,CHECK_ID,TEST_NAME,ELASTICSEARCH,STATUS,PERSON'
        sql = 'SELECT '+my_query+' FROM '+self.table_name
        sql = sql + ' WHERE JOB_ID = '+str(_job_id)
        try:
            cursor.execute(sql)
            output = cursor.fetchall()
        except MySQLdb.Error, e:
            print "Something Went Wrong: %s " % str(e)
            return
        finally:
            db.close()

        headers = {}
        for n,h in enumerate(my_query.split(',')):
            headers[h] = n
        _status = {}
        _person = {}

        for row in output:
            my_test   = row[headers['TEST_NAME']]
            my_status = row[headers['STATUS']]
            my_person = row[headers['PERSON']]
            my_es     = row[headers['ELASTICSEARCH']]
            if ( 'ZERO_DOLLAR_STORE'     == my_test or 
                 'BLANK_CHECK'           == my_test or 
                 'NEGATIVE_DOLLAR_STORE' == my_test
               ):
                _status[my_test+'|'+my_es] = my_status
                _person[my_test+'|'+my_es] = my_person

        return(_status,_person)



    # ###################################
    #  get whether this date is the latest loadid
    def get_is_latest_loadid(self,_retailer,_year,_month,_week,_load_id):
       _load_id = int(_load_id)
       #db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
       cursor = self.get_cursor() #db.cursor()
       sql = 'SELECT RETAILER_ID,YEAR,MONTH,WEEK,LOAD_ID FROM '+self.table_name
       sql = sql + (' WHERE RETAILER_ID='+str(_retailer)+' and YEAR='+str(_year)+
                    ' and MONTH='+str(_month)+' and WEEK='+str(_week)+';')

       print(sql)
       cursor.execute(sql)
       output = cursor.fetchall()
       db.close()

       headers = {}
       for n,h in enumerate(sql.split(' ')[1].split(',')):
            headers[h] = n

       max_load_id = -1
       for row in output:
           print(row)
           this_loadid = int(row[headers['LOAD_ID']])
           if max_load_id < this_loadid:
               max_load_id = this_loadid

       if len(output) == 1:
           print(str(max_load_id)+' '+str(_load_id))
           if max_load_id != _load_id:
               print('SERIOUS ERROR: load id mismatch')
               return(False)
           return(True)
       if max_load_id > _load_id:
           return(False)
       return(True)

    ############################
    ##
    ## Get the latest action and status for a retailer/date
    ##
    def get_date_format(self):
        return('%Y-%m-%d %H:%M:%S')
    ############################

    ############################
    ##
    ## Get the latest action and status for a retailer/date
    ##
    def get_current_time(self):
        return(datetime.strftime(datetime.now(),
                                 self.get_date_format()))
    ############################




