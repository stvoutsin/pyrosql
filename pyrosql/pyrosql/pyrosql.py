'''
Created on Jun 4, 2018

@author: stelios
'''
from __future__ import generators    # needs to be at the top of your module

import os
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
from .db_helper import DBHelper
from .table import Table

def ResultIter(cursor, arraysize=100000):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result

            
class PyroSQL(object):
    '''
    SQLEngine
    Class that handles database interactions
    '''


    def __init__(self, dbserver, dbuser, dbpasswd, dbport='1433', driver='TDS'):
        '''
        Constructor   
           
        :param dbserver:
        :param dbuser:
        :param dbpasswd:
        :param dbport:
        :param driver:
        '''
     
        self.dbserver = dbserver
        self.dbuser = dbuser
        self.dbpasswd = dbpasswd
        self.dbport = dbport
        self.driver = driver
        

    def _get_rows(self, query_result):
        '''
        Get rows from a query result array
        
        :param query_result:
        '''
        row_length = None
        if len(query_result)>=2:
            row_length = len(query_result[1])    
        return row_length
   

    def execute_sql_query(self, query, database, limit=None, timeout=None):
        '''
        Execute an SQL query
        
        @param query: The SQL Query
        @param database: The Database
        @param limit: limit (rows)
        @param database: timeout
        '''
        mydb = DBHelper(self.dbserver, self.dbuser, self.dbpasswd, self.dbport, self.driver)
        table_data = mydb.execute_query_get_cols_rows(query, database, limit, timeout)
        return Table(table_data[1], table_data[0])
            
    
    def execute_update(self, query, database):
        '''
        Execute an SQL Update
        
        @param query: The SQL Query
        @param database: The Database
        '''
        mydb = DBHelper(self.dbserver, self.dbuser, self.dbpasswd, self.dbport, self.driver)
        response = mydb.execute_update(query, database)
        return response
        

    def execute_insert (self, qry, database, params):
        '''
        Execute an insert query (qry) against a db 
        
        :param qry:
        :param database:
        :param params:
        '''
        mydb = DBHelper(self.dbserver, self.dbuser, self.dbpasswd, self.dbport, self.driver)
        res = mydb.execute_insert(qry, database, params)
        return res
    

    def execute_query_empty (self, qry, database, limit=None, timeout=None):
        '''
        Execute a query (qry) against a db and table

        :param qry:
        :param database:
        '''
        mydb = DBHelper(self.dbserver, self.dbuser, self.dbpasswd, self.dbport, self.driver)
        table_data = mydb.execute_qry_no_results(qry, database, limit, timeout)
        return table_data
          
 
