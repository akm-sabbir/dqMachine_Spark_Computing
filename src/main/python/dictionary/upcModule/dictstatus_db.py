
############################
############################
## 
## Job Status controller for DICTIONARY
## 
############################
############################

#from collections import Counter,defaultdict,namedtuple
import MySQLdb
import sys
import subprocess
from datetime import datetime
import os
import appDebug
from appDebug import applicationDebugModule
class dictstatus_db:
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
   # ##################################


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
    def set_changeid(self,_itemid,_poiid,_filename = '', _update_date='' , _quiet=False):
        # Connect to DB
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

        # Define the uniqueness
        columns = ['ITEMID','POIID','FILE','UPDATED_DATE']
        values  = [str(_itemid),str(_poiid),str(_filename),str(_update_date)]

        # Form a query
        sql = 'SELECT '+self.JobID_name+' FROM '+self.table_name+' WHERE '
        for i,column in enumerate(columns):
            if column == 'FILE' and values[i] == '':
                continue
            if column=='UPDATED_DATE' and values[i]=='':
                continue
            if i > 0:
                sql = sql+' AND '
            sql = sql+column+'="'+str(values[i])+'"'

        # Test whether this is unique
        job_id = self.test_exists(cursor, sql)

        # A valid job_id means that this is not unique, pass back the job_id
        if job_id >= 0:
            if _quiet == False:
                print('This already exists: '+str(job_id))
            self.db_JobID = job_id
            self.db_JobIsNew = False
            db.close()
            return(job_id)

        # We did not find an entry like this, pass back our null flag
        db.close()
        return(-1)

    # ###################################
    #  Find the next file not processed
    def get_last_file(self,_type):
        # Initiate db connection
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

        sql = 'SELECT distinct(FILE) FROM '+self.table_name+' '
        sql = sql+'WHERE ITEM_TYPE="'+_type+'" order by FILE desc limit 1;'
        
        cursor.execute(sql)
        results = cursor.fetchall()
        last_file = results[0][0]

        next_file = last_file.split('-m-')[0]+'-m-'
        filenumber = int(last_file.split('-m-')[1].replace('.gz',''))
        next_file = next_file+'{:05}'.format(filenumber + 1 ) + '.gz'
        return(last_file)
        

    # ###################################
    #  Start a new change_id in the DB
    #  If this is not unique, then we will skip insert
    #  and pass back the original job_id
    def start_new_changeid_dict(self,_data,## dictionary of lists
                                _change_type,
                                _item_type,
                                _file='No file given',
                                _quiet=False):
        
        # Initiate db connection
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

        # Set the place in the row where we find our infomation
        column_names = {}
        if _item_type == 'ITEMID':
            column_names = {'ITEMID':'item_id','BUSINESSID':'business_id',
                            'SUBCAT':'subcategory_number','ITEMNUMBER':'item_number',
                            'ADDED_DATE':'added_date','UPDATE_DATE':'update_date',
                            'DURATION':'Duration'}
        if _item_type == 'POIID':
            column_names = {'ITEMID':'item_id','BUSINESSID':'business_id',
                            'POIID':'poiid','POSOUTLET':'posoutlet',
                            'OUTLETBRAND':'outletbrand','OUTLETITEMNUMBER':'outletitemnumber',
                            'OUTLETDESCRIPTION':'outletdescription','SKU':'sku',
                            'MANUFACTURERCODE':'manufacturercode',
                            'ADDED_DATE':'added_date','UPDATE_DATE':'update_date',
                            'DURATION':'Duration'}


        error_found = False
        for name in column_names.values():
            if name not in _data:
                print('Error '+name+' is not found in data')
                error_found = True
        if error_found:
            print('I cannot go on ...')
            return(-1)
            
        businessid = '-1'
        if 'BUSINESSID' in column_names:
            businessid = _data[column_names['BUSINESSID']]
        else:
            print('No business id found, bailing')
            return(-1)
        
        # Define the variables
        subcat      = ['-1']*len(businessid)
        itemnumber  = ['-1']*len(businessid)
        poiid       = ['-1']*len(businessid)
        itemid      = ['-1']*len(businessid)
        added_date  = ['-1']*len(businessid)
        update_date = ['-1']*len(businessid)
        duration    = ['-1']*len(businessid)
        if 'SUBCAT'       in column_names:
            subcat      = _data[column_names['SUBCAT']]
        if 'ITEMNUMBER'   in column_names:
            itemnumber  = _data[column_names['ITEMNUMBER']]
        if 'POIID'        in column_names:
            poiid       = _data[column_names['POIID']]
        if 'ITEMID'       in column_names:
            itemid      = _data[column_names['ITEMID']]
        if 'ADDED_DATE'   in column_names:
            added_date  = _data[column_names['ADDED_DATE']]
        if 'UPDATE_DATE'  in column_names:
            update_date = _data[column_names['UPDATE_DATE']]
        if 'DURATION'     in column_names:
            duration    = _data[column_names['DURATION']]

        total_to_process = len(itemid)
        my_next = 10
        int_columns = ['BUSINESS_ID',
                       'SUB_CATEGORY',
                       'ITEMNUMBER',
                       'ITEMID',
                       'POIID']
        columns = ['ITEM_TYPE','CHANGE_TYPE','ADDED_DATE','UPDATED_DATE','FILE']
        insert_sql_start = 'INSERT INTO '+self.table_name+'('+','.join(columns)+','+','.join(int_columns)+',FINDSTATUS_END) VALUES'
        insert_sql = ''
        
        for i,item_id in enumerate(itemid):
            if i%10 == 0:
                if 'VALUES (' in insert_sql:
                    cursor.execute(insert_sql)
                    db.commit()
                print(insert_sql)
                insert_sql = insert_sql_start
                
            if (100*i/total_to_process)>my_next:
                my_next += 10
                print(str(i)+' out of '+str(total_to_process)+' completed')

            # Check if this is a unique entry
            job_id = self.set_changeid(itemid[i],poiid[i],'',update_date[i],_quiet=_quiet)

            # NOT unique, pass back the job_id
            if job_id >= 0:
                continue

            # Is unique, insert this into the DB
            int_values  = [str(businessid[i]),
                           str(subcat[i]),
                           str(itemnumber[i]),
                           str(itemid[i]),
                           str(poiid[i])]
            values  = [str(_item_type),str(_change_type),str(added_date[i]),str(update_date[i]),str(_file)]
            if 'VALUES (' in insert_sql:
                insert_sql = insert_sql+','
            insert_sql = insert_sql+' ("'+'","'.join(values)+'",'+','.join(int_values)+',NOW())'
        
        if 'VALUES (' in insert_sql:
            cursor.execute(insert_sql)
            db.commit()

        db.close()

        return(1)


    # ###################################
    #  Start a new change_id in the DB
    #  If this is not unique, then we will skip insert
    #  and pass back the original job_id
    def start_new_changeid(self,
                           _filename,
                           _row,
                           _change_type='UNKNOWN',
                           _quiet=False):
        print('Need to fix to add in added and updated dates')
        # We only consider these two tables for now
        if ( 'ODS_POSITEMS' not in _filename and
             'ODS_POSOUTLETITEMS' not in _filename):
            print('dictstatus_db::start_new_changeid\n'+
                  'Filename not recognized')
            return(-1)

        # Initiate db connection
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

        # Set the place in the row where we find our infomation
        item_type = 'UNKNOWN'
        column_names = {}
        if 'ODS_POSITEMS' in _filename:
            item_type = 'ITEMID'
            column_names = {'ITEMID':0,'BUSINESSID':1,
                            'SUBCAT':2,'ITEMNUMBER':3,
                            'ADDED':105, 'UPDATED':106}
        if 'ODS_POSOUTLETITEMS' in _filename:
            item_type = 'POIID'
            column_names = {'ITEMID':20,'BUSINESSID':1,
                            'POIID':0,'POSOUTLET':2,
                            'OUTLETBRAND':8,'OUTLETITEMNUMBER':9,
                            'OUTLETDESCRIPTION':10,'SKU':14,
                            'MANUFACTURERCODE':16,
                            'ADDED':26, 'UPDATED':27}

        # Split the row
        myrow = _row.strip().split('|')

        # Define the variables
        subcat = '-1'
        itemnumber = '-1'
        poiid = '-1'
        if 'SUBCAT' in column_names:
            subcat     = myrow[column_names['SUBCAT']]
        if 'ITEMNUMBER' in column_names:
            itemnumber = myrow[column_names['ITEMNUMBER']]
        if 'POIID' in column_names:
            poiid = myrow[column_names['POIID']]

        # Check if this is a unique entry
        job_id = self.set_changeid(myrow[column_names['ITEMID']],
                                   poiid,
                                   _filename,
                                   _quiet=False)

        # NOT unique, pass back the job_id
        if job_id >= 0:
            self.db_JobID = job_id
            self.db_JobIsNew = False
            db.close()
            return(job_id)

        # Is unique, insert this into the DB
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

        # Make sure we set the job_id
        self.db_JobID = self.set_changeid(myrow[column_names['ITEMID']],
                                          poiid,
                                          _filename,
                                          _quiet=True)

        # return the job_id
        return(self.db_JobID)

    # ###################################
    #  Test whether the selection already exists in the DB
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
    #  Set the details in the POIID_DETAILS table
    def set_poiid_details(self,
                          _job_id,
                          _row):

        # Initiate db connection
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

        # define where the data is in the row
        column_names = {}
        column_names = {'ITEMID':20,'BUSINESSID':1,
                        'POIID':0,'POSOUTLET':2,
                        'OUTLETBRAND':8,'OUTLETITEMNUMBER':9,
                        'OUTLETDESCRIPTION':10,'SKU':14,
                        'MANUFACTURERCODE':16}

        # split the row
        myrow = _row.strip().split('|')

        # Define the columns we want to use
        int_columns = ['CHANGE_ID','POSOUTLET','UPC']
        columns = ['SKU','OUTLETDESCRIPTION',
                   'OUTLETITEMNUMBER','OUTLETBRAND']

        int_values  = [str(_job_id),str(myrow[column_names['POSOUTLET']]),
                       str(myrow[column_names['MANUFACTURERCODE']])]
        values  = [str(myrow[column_names['SKU']]),
                   str(myrow[column_names['OUTLETDESCRIPTION']]),
                   str(myrow[column_names['OUTLETITEMNUMBER']]),
                   str(myrow[column_names['OUTLETBRAND']])]

        # Write an SQL command
        sql = 'SELECT CHANGE_ID FROM ' + self.table_name + ' WHERE '
        for i,int_column in enumerate(int_columns):
            if i > 0:
                sql = sql+' AND '
            sql = sql+int_column + '="' + str(int_values[i]) + '"'
        for i,column in enumerate(columns):
            if i > 0 or len(int_column) > 0:
                sql = sql + ' AND '
            sql = sql+column + '="' + str(values[i]) + '"'

        # If this has been added before, skip, otherwise insert
        if self.test_exists(cursor,sql) < 0:
            insert_sql = 'INSERT INTO ' + self.table_name + '(' + ','.join(columns)+',' + ','.join(int_columns)+')'
            insert_sql = insert_sql + ' VALUES("' + '","'.join(values)+ '",' + ','.join(int_values) + ')'
            cursor.execute(insert_sql)
            db.commit()
            
        db.close()


    
    # ###################################
    #  Get Status (internal function)
    def get_status(self, _cursor, _column_name):
        if self.db_JobID < 0:
            print('No Change ID found')
            return(-1)
        sql = 'SELECT '+_column_name+' FROM '+self.table_name+' '
        sql = sql+'WHERE '+self.JobID_name+' = '+str(self.db_JobID)+';'
        
        _cursor.execute(sql)
        results = _cursor.fetchall()
        
        return(results[0][0])

    # ###################################
    #  Get Status (external function)
    def get_status_force(self, _column_name):
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()
        
        sql = 'SELECT ' + _column_name + ' FROM ' + self.table_name + ' '
        sql = sql + 'WHERE ' + self.JobID_name + ' = ' + str(self.db_JobID) + ';'

        cursor.execute(sql)
        results = cursor.fetchall()

        db.close()
        return(results[0][0])


    # ###################################
    #  Update field (internal function)
    def update(self, _db, _cursor, _column_name, _value):
        sql = 'UPDATE ' + self.table_name + ' '
        sql = sql + 'SET '+ _column_name + ' = ' + str(_value) + ', '
	sql = sql + 'LATESTADDFLAG = 0'  
        sql = sql + 'WHERE ' + self.JobID_name+' = ' + str(self.db_JobID) + ';'
        
        _cursor.execute(sql)
        _db.commit()

    def update_mulitple(self, _db, _cursor,_type, **params):
	sql = 'UPDATE' + self.table_name + ' '
        sql = sql + 'SET ' 
	for each,ind in enumerate(params.iteritems()):
		sql = sql + each[0] + ' = ' + str(each[1]) 
		sql  = sql + ', ' if ind is not len(params.iteritems())-1 else sql + ' '
	sql = sql + ' WHERE ITEM_TYPE ="' + _type+'";'
	_cursor.execute(sql)
	_db.commit()
	return

    def performDbUpdate(self, db,cursor):
        def doUpate(id_):
            self.db_jobID = id_
            self.update(db,cursor, 'UPC_SEARCHSTATUS_START', 'CURRENT_TIMESTAMP')
            return 1
        return doUpdate
    # ###################################
    #  Start UPC search
    #  return a list of upcs
    # retrieving poiids to build related upc dictionary 
    # retrieving  sku, upc, brand and model from mysql server database
    def start_upc_search(self, _process_name):
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()
        # Change IDs for POIID only
        sqlJ = 'SELECT '+ self.JobID_name + ' FROM ' + self.table_name+'  WHERE LATESTADDFLAG = 1 and ITEM_TYPE = "POIID" '
    
        #cursor.execute(sqlJ)
        #results = cursor.fetchall()

        sql = 'SELECT PD.CHANGE_ID, UPC,SKU,POSOUTLET,OUTLETITEMNUMBER,OUTLETBRAND,PD.CHANGE_ID FROM  POIID_DETAILS  as PD inner join (' + sqlJ + ') as DQS on (PD.LATESTADDLAG = 1 and  PD.POIID = DQS.POIID);'
        #sql = sql+'WHERE CHANGE_ID in ('+','.join(str(x) for x in res_list)+');'
        cursor.execute(sql)
        results = cursor.fetchall()
        if len(results) is 0:
		applicationDebugModule(msg = "database is empty for change_id no new adds found\n this is under dictstatus_db module in start_upc_search function")	
		sys.exit()
		return
        res_list = [data if  data is not None else 0 for data in list(zip(*results)[0])]
	upcs = [data if data is not None else 0 for data in list(zip(*results)[1])]
        skus = [data if data is not None else 0 for data in list(zip(*results)[2])]
        rtrs = [data if data is not None else 0 for data in list(zip(*results)[3])]
        mods = [data if data is not None else 0 for data in list(zip(*results)[4])]
        bnds = [data if data is not None else 0 for data in list(zip(*results)[5])]
        tempUpdate = self.performDbUpdate(db,cursor)
        map(tempUpdate,list(zip(*results)[0]))
        db.close()
        return({'ChID':res_list,'UPC':upcs,'SKU':skus,'POSOUTLET':rtrs,'MOD':mods,'BRAND':bnd_list})

    # ###################################
    #  End a process
    def end_process(self, _process_name):
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

        self.update(db, cursor, _process_name + 'STATUS_END', 'CURRENT_TIMESTAMP')

        db.close()

    # ###################################
    #  Anomalies Found
    def set_anomalies(self, _process_name, _process_anomaly, _number):
        print('set_anomalies is not yet defined')
        return(0)
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
        print('set_status_code is not yet defined')
        return(0)
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
        print('set_status_code_force is not yet defined')
        return(0)
        db = MySQLdb.connect(self.hostname,'root','My$qlD',self.db_name)
        cursor = db.cursor()

        status = self.get_status(cursor,_process_name+'_STATUS')

        self.update(db,cursor,_process_name+'_'+_code_name,'"'+_code+'"')

        db.close()

    # ###################################
    #  Set a free-form comment
    def set_comment(self, _process_name, _comment_name, _comment):
        print('set_comment is not yet defined')
        return(0)
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
        print('set_value is not yet defined')
        return(0)
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




