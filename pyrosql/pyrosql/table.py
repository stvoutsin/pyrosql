'''
Created on Feb 8, 2018

@author: stelios
'''
import logging
from astropy.table import Table as astropy_Table


class Table(object):
    """
    Table class, contains methods for accessing data for astronomy use (i.e. get as Astropy Table

    """


    def __init__(self, data, columns, maxrows=100000000):
        """
        Constructor
        """ 
        self.data = data
        self.columns = columns
        self.maxrows = maxrows
        return

        
    def count(self):
        """Get Row count
        
        Returns
        -------
        rowcount: integer
            Count of rows  
        """  
        rowcount = -1        
        try:
            rowcount = len(self.data)
        except Exception as e:
            logging.exception(e) 
               
        return rowcount
    
    
    def as_astropy (self, limit=False):
        """Get Astropy table
                             
        Returns
        -------
        astropy_table: Astropy.Table
            Table as Astropy table 
        """
        if (limit):
            if (self.count()>self.maxrows):
                raise Exception ("Max row limit exceeded")
            else :
                return astropy_Table(rows=self.data, names=self.columns)
        else:
            return astropy_Table(rows=self.data, names=self.columns)        
 
