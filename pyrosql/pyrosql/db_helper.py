from __future__ import generators    # needs to be at the top of your module

import os
import urllib.request
import time
import pyodbc
try:
    import simplejson as json
except ImportError:
    import json
import numpy
import re
import datetime
from time import gmtime,  strftime
import logging
import datetime


class DBHelper:
    '''
    DBHelper module
    Class to provide necessary database functionality

    University of Edinburgh, WAFU
    @author: Stelios Voutsinas
    
    '''
    
    def __init__(self, db_server, username, password, port="1433", driver="TDS"):
        '''
        Initialise DBHelper class instance
        '''
        self.db_server = db_server
        self.username = username
        self.password = password
        self.port = port
        self.driver = driver
        
    def execute_qry_single_row (self, query, db_name):
        '''
        Execute a query on a database & table, that will return a single row
        '''
        return_val = []
        params = 'DRIVER={' + self.driver + '};SERVER=' + self.db_server + ';Database=' + db_name +';UID=' + self.username + ';PWD=' + self.password +';TDS_Version=8.0;Port='  + self.port + ';'
        cnxn = pyodbc.connect(params)  
        cursor = cnxn.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        
        if row:
            return_val = row

        cnxn.close()
        
        return return_val
    
    def execute_qry_no_results (self, query, db_name, limit=None, timeout=None):
        '''
        Execute a query on a database & table, that will return a single row
        '''
        return_val = []
        params = 'DRIVER={' + self.driver + '};SERVER=' + self.db_server + ';Database=' + db_name +';UID=' + self.username + ';PWD=' + self.password +';TDS_Version=8.0;Port='  + self.port + ';'
        cnxn = pyodbc.connect(params)
        cursor = cnxn.cursor()
        cursor.execute(query)
        cnxn.close()

        return

    
    def execute_query_multiple_rows(self, query, db_name, limit=None, timeout=None):
        '''
        Execute a query on a database & table that may return any number of rows
        '''
        return_val = []
        params = 'DRIVER={' + self.driver + '};SERVER=' + self.db_server + ';Database=' + db_name +';UID=' + self.username + ';PWD=' + self.password +';TDS_Version=8.0;Port='  + self.port + ';'

        cnxn = pyodbc.connect(params)  

        if timeout!=None:
            cnxn.timeout=timeout

        cursor = cnxn.cursor()
        cursor.execute(query)

       
        if (limit!=None):
            rows = cursor.fetchmany(limit)
        else :
            rows = cursor.fetchall()


        for row in rows:
            return_val.append(row)

        cnxn.close()
        
        return return_val
        
        
        
    def execute_query_get_cols_rows(self, query, db_name, limit=None, timeout=None):
        '''
        Execute a query on a database & table that may return any number of rows
        '''
        return_val = []
       
        params = 'DRIVER={' + self.driver + '};SERVER=' + self.db_server + ';Database=' + db_name +';UID=' + self.username + ';PWD=' + self.password +';TDS_Version=8.0;Port='  + self.port + ';'
        print (params)
        cnxn = pyodbc.connect(params)  

        if timeout!=None:
            cnxn.timeout=timeout
        try:
            cursor = cnxn.cursor()
            cursor.execute(query)

            if cursor.description!=None:
                columns = [column[0] for column in cursor.description]
            else:
                columns=[]
            return_val.append(columns)
            rowlist=[]

            if (limit!=None):
                rows = cursor.fetchmany(limit)
            else :
                rows = cursor.fetchall()


            for row in rows:
                rowlist.append(dict(zip(columns, row)))
            
            return_val.append(rowlist) 
            cnxn.close()

        except Exception as e:
            logging.exception(e)
            #cnxn.close()
            raise e
            
        return return_val


    def execute_insert (self, insert_query, db_name, query_parameters):           
        '''
        Execute an insert on a database & table 
        '''
        return_val = []
        params = 'DRIVER={' + self.driver + '};SERVER=' + self.db_server + ';Database=' + db_name +';UID=' + self.username + ';PWD=' + self.password +';TDS_Version=8.0;Port='  + self.port + ';'
        cnxn = pyodbc.connect(params)  
        cursor = cnxn.cursor()
        cursor.execute(insert_query, query_parameters)
        cnxn.commit()
        cnxn.close()
        
    def execute_update(self, update_query, db_name):           
        '''
        Execute an insert on a database & table 
        '''
        return_val = []
        params = 'DRIVER={' + self.driver + '};SERVER=' + self.db_server + ';Database=' + db_name +';UID=' + self.username + ';PWD=' + self.password +';TDS_Version=8.0;Port='  + self.port + ';'
        cnxn = pyodbc.connect(params)  
        cursor = cnxn.cursor()
        cursor.execute(update_query)
        cnxn.commit()
        cnxn.close()        
            
            

