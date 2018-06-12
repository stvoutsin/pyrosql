'''
Created on Feb 8, 2018

@author: stelios
'''
import logging
from astropy.table import Table as astropy_Table


class Table(Object):
    """
    classdocs
    """


    def __init__(self, data, maxrows=100000000):
        """
        Constructor
        """ 
        self.data = data
        self.maxrows = maxrows
        return

        
    def count(self):
        """Get Row count
        
        Returns
        -------
        rowcount: integer
            Count of rows  
        """  
        rowcount = None        
        try:
            rowcount = count(data)
        except Exception as e:
            logging.exception(e) 
               
        return rowcount
    
    
    def as_astropy (self, limit=True):
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
                return astropy_Table.read()
        else:
            return astropy_Table.read()        
 
