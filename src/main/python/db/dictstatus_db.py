
############################
############################
## 
## Job Status controller for DICTIONARY
## 
############################
############################
#package src.main.python.db
#from collections import Counter,defaultdict,namedtuple
import MySQLdb

import subprocess
from datetime import datetime
import os

class dictstatus_db(object):
    """
       Job Status Controller for DICTIONARY 
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
        return
   # ##################################
    def methods(self):
        listOfFuncs = ["table_stats","set_hostname","set_database","set_primary_key_name","set_table","get_primary_key_name","set_table","get_primary_id","set_changeid","start_new_changeid","test_exists","get_setup"\
                , "get_is_new", "set_poiid_details", "get_status", "get_status_force", "update", "start_process","end_process","set_anomalies", "set_status_code", "set_status_code_force","set_comment","set_value","Print",\
                "get_current_time"]
        for each in listOfFuncs:
            print each
        return listOfFuncs


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


    # ###################################
    #  Set the job id based on the input
    # NEED TO IMPLEMENT THE _FILETYPE CHECK
    def set_changeid(self,_itemid,_poiid,_filename,
                     _quiet=False):
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

        columns = ['ITEMID','POIID','FILE']
        values  = [str(_itemid),str(_poiid),str(_filename)]

        sql = 'SELECT '+self.JobID_name+' FROM '+self.table_name+' WHERE '
        for i,column in enumerate(columns):
            if i > 0:
                sql = sql+' AND '
            sql = sql+column+'="'+str(values[i])+'"'

        job_id = self.test_exists(cursor, sql)
        if job_id >= 0:
            if _quiet == False:
                print('This already exists: '+str(job_id))
            self.db_JobID = job_id
            self.db_JobIsNew = False
            db.close()
            return(job_id)

        db.close()
        return(-1)

    # ###################################
    #  Set methods for each items
    def start_new_changeid(self,
                           _filename,
                           _row,
                           _change_type='UNKNOWN',
                           _quiet=False):
        if ( 'ODS_POSITEMS' not in _filename and
             'ODS_POSOUTLETITEMS' not in _filename):
            print('dictstatus_db::start_new_changeid\n'+
                  'Filename not recognized')
            return(-1)

        # Initiate db connection
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

        item_type = 'UNKNOWN'
        column_names = {}
        if 'ODS_POSITEMS' in _filename:
            item_type = 'ITEMID'
            column_names = {'ITEMID':0,'BUSINESSID':1,
                            'SUBCAT':2,'ITEMNUMBER':3}
        if 'ODS_POSOUTLETITEMS' in _filename:
            item_type = 'POIID'
            column_names = {'ITEMID':20,'BUSINESSID':1,
                            'POIID':0,'POSOUTLET':2,
                            'OUTLETBRAND':8,'OUTLETITEMNUMBER':9,
                            'OUTLETDESCRIPTION':10,'SKU':14,
                            'MANUFACTURERCODE':16}

        myrow = _row.strip().split('|')

        subcat = '-1'
        itemnumber = '-1'
        poiid = '-1'
        if 'SUBCAT' in column_names:
            subcat     = myrow[column_names['SUBCAT']]
        if 'ITEMNUMBER' in column_names:
            itemnumber = myrow[column_names['ITEMNUMBER']]
        if 'POIID' in column_names:
            poiid = myrow[column_names['POIID']]
            
        job_id = self.set_changeid(myrow[column_names['ITEMID']],
                                   poiid,
                                   _filename,
                                   _quiet=False)

        if job_id >= 0:
            self.db_JobID = job_id
            self.db_JobIsNew = False
            db.close()
            return(job_id)

        self.db_JobIsNew = True
        int_columns = ['BUSINESS_ID','SUB_CATEGORY',
                       'ITEMNUMBER',
                       'ITEMID','POIID']
        columns = ['ITEM_TYPE','CHANGE_TYPE','FILE']

        int_values  = [myrow[column_names['BUSINESSID']],str(subcat),
                       str(itemnumber),
                       str(myrow[column_names['ITEMID']]),str(poiid)]
        values  = [item_type,str(_change_type),str(_filename)]
        insert_sql = 'INSERT INTO '+self.table_name+'('+','.join(columns)+','+','.join(int_columns)+',FINDSTATUS_END)'
        insert_sql = insert_sql+' VALUES("'+'","'.join(values)+'",'+','.join(int_values)+',NOW())'
        
        cursor.execute(insert_sql)
        db.commit()
        db.close()
        self.db_JobID = self.set_changeid(myrow[column_names['ITEMID']],
                                          poiid,
                                          _filename,
                                          _quiet=True)
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
    #  
    def set_poiid_details(self,
                          _job_id,
                          _row):

        # Initiate db connection
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

        column_names = {}
        column_names = {'ITEMID':20,'BUSINESSID':1,
                        'POIID':0,'POSOUTLET':2,
                        'OUTLETBRAND':8,'OUTLETITEMNUMBER':9,
                        'OUTLETDESCRIPTION':10,'SKU':14,
                        'MANUFACTURERCODE':16}

        myrow = _row.strip().split('|')

        int_columns = ['CHANGE_ID','POSOUTLET','UPC']
        columns = ['SKU','OUTLETDESCRIPTION',
                   'OUTLETITEMNUMBER','OUTLETBRAND']

        int_values  = [str(_job_id),str(myrow[column_names['POSOUTLET']]),
                       str(myrow[column_names['MANUFACTURERCODE']])]
        values  = [str(myrow[column_names['SKU']]),
                   str(myrow[column_names['OUTLETDESCRIPTION']]),
                   str(myrow[column_names['OUTLETITEMNUMBER']]),
                   str(myrow[column_names['OUTLETBRAND']])]

        sql = 'SELECT CHANGE_ID FROM '+self.table_name+' WHERE '
        for i,int_column in enumerate(int_columns):
            if i > 0:
                sql = sql+' AND '
            sql = sql+int_column+'="'+str(int_values[i])+'"'
        for i,column in enumerate(columns):
            if i > 0 or len(int_column)>0:
                sql = sql+' AND '
            sql = sql+column+'="'+str(values[i])+'"'

        if self.test_exists(cursor,sql)<0:
            insert_sql = 'INSERT INTO '+self.table_name+'('+','.join(columns)+','+','.join(int_columns)+')'
            insert_sql = insert_sql+' VALUES("'+'","'.join(values)+'",'+','.join(int_values)+')'
            cursor.execute(insert_sql)
            db.commit()
            
        db.close()


    
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
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()
        
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
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

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
        return()##Needs fixing
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

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
    #  Anomalies Found
    def set_anomalies(self, _process_name, _process_anomaly, _number):
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

        status = self.get_status(cursor,_process_name+'_STATUS')
        if status != 'COMPLETE':
            print(_process_name+' has '+status+' for job '+str(self.db_JobID)+' no update is made')
            db.close()
            return(-1)

        self.update(db,cursor,_process_name+'_'+_process_anomaly,str(_number))

        db.close()

    # ###################################
    #  Set a status code
    def set_status_code(self, _process_name, _code_name, _code):
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

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
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

        status = self.get_status(cursor,_process_name+'_STATUS')

        self.update(db,cursor,_process_name+'_'+_code_name,'"'+_code+'"')

        db.close()

    # ###################################
    #  Set a free-form comment
    def set_comment(self, _process_name, _comment_name, _comment):
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

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
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

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
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

        sql = 'SELECT * FROM '+self.table_name
        sql = sql + ' WHERE '+self.JobID_name+' = '+str(self.db_JobID)

        cursor.execute(sql)
        output = cursor.fetchall()
        for row in output:
            print(row)
        
        if len(output) == 1:
            print('Only '+str(len(output))+' row is found')
        else:
            print('There are '+str(len(output))+' rows found') 
        db.close()


    ############################
    ##
    ## Get the latest action and status for a retailer/date
    ##
    def get_current_time(self):
        return(datetime.strftime(datetime.now(),
                                 self.get_date_format()))
    ############################




