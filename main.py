import sys
import os
import csv
import re
import operator
import sqlparse
from collections import OrderedDict

class MiniSQL:
    def __init__(self):
        super().__init__()
        self.tableInfo = OrderedDict()
        self.database = OrderedDict()
        self.getMetaInfo()
        self.fillContent()
    
    def getMetaInfo(self):
        try:
            infoFile = open('metadata.txt', 'r')
        except FileNotFoundError as fn:
            print("metadata.txt not found")
            raise
        except FileExistsError as fn:
            print("metadata.txt does not exist")
            raise
        else:
            tableStarted = False
            tableName = ""
            for row in infoFile:
                if row.strip() == '<begin_table>':
                    tableStarted = True
                    continue
                if row.strip() == '<end_table':
                    continue
                if tableStarted:
                    tableStarted = False
                    tableName = row.strip()
                    self.tableInfo[tableName] = []
                    continue
                # append the column names into the table dict
                self.tableInfo[tableName].append(row.strip())
    
    @staticmethod
    def getCSV(name):
        """
        Provides the content of the CSV file
        """
        content = []
        try:
            tabFile = open(name, 'r')
        except Exception as e:
            print(name + " file does not exist")
            raise FileExistsError
        else:
            for row in tabFile:
                content.append(row.strip("\r\n"))
            return content
    
    def fillContent(self):
        # First delete those tableInfo entries whose corresponding files are not present
        for table in self.tableInfo:
            if os.path.isfile(os.getcwd() + str(table) + ".csv") == False:
                del self.tableInfo[table]

        # Initialise the database
        for table in self.tableInfo:
            self.database[table] = OrderedDict()
            for column in self.tableInfo[table]:
                self.database[table][column] = [] # Each column has a list of data
        
        # Finally fill the content in database
        for table in self.tableInfo:
            rows = getCSV(str(table) + ".csv")
            for row in rows:
                data = row.split(',')
                for i in range(len(data)):
                    colName = self.tableInfo[table][i]
                    d = data[i].strip()
                    d = d.strip('\n')
                    self.database[table][colName].append(int(d))


    




