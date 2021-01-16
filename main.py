import sys
import os
import csv
import re
import sqlparse
import functools
from collections import OrderedDict

class MiniSQL:
    def __init__(self):
        self.tableInfo = OrderedDict()
        self.database = OrderedDict() # database[TABLE_NAME][COLUMN_NAME] -> gives list of values in this column
        self.getMetaInfo()
        self.fillContent()
    
    def getMetaInfo(self):
        """
        Reads the metadata.txt file to read in the schema of the database.
        Each table in database is stored in 
        """
        try:
            infoFile = open('metadata.txt', 'r')
        except FileNotFoundError as fn:
            print("metadata.txt not found")
            raise fn
        else:
            tableStarted = False
            tableName = ""
            for ro in infoFile:
                if ro.strip() == '<begin_table>':
                    tableStarted = True
                    continue
                if ro.strip() == '<end_table>':
                    continue
                if tableStarted:
                    tableStarted = False
                    tableName = ro.strip()
                    self.tableInfo[tableName] = []
                    continue
                # append the column names into the table dict
                self.tableInfo[tableName].append(ro.strip())
    
    @staticmethod
    def getCSV(name):
        """
        Provides the content of the CSV file
        """
        try:
            tabFile = open(name, 'r')
        except Exception as e:
            print(name + " file does not exist")
            raise FileExistsError
        else:
            content = []
            first = True # we do not have to read the first line of csv file
            for row in tabFile:
                if first:
                    first = False
                    continue
                content.append(row.strip("\r\n"))
            return content
    
    def fillContent(self):
        """
        Fills in the content in the database
        """
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

    def aggregate(self, tableName, column, fun):
        """
        Gives the aggregate function 'fun' on 'tableName' for 'column'
        """
        if tableName not in self.tableInfo.keys():
            raise FileNotFoundError(str(tableName) + " table does not exist in the database")
        if column not in self.tableInfo[tableName]:
            raise NotImplementedError(str(tableName) + " table does not have any column named " + str(column))

        if fun == 'MAX':
            val = int(-1e9)
            for v in self.database[tableName][column]:
                val = max(val, v)
            return val
        elif fun == 'MIN':
            val = int(1e9)
            for v in self.database[tableName][column]:
                val = min(val, v)
            return val
        elif fun == 'COUNT':
            return len(self.database[tableName][column])
        elif fun == 'SUM':
            return functools.reduce(lambda a,b : a + b, self.database[tableName][column])
        elif fun == 'AVG':
            summ = functools.reduce(lambda a,b : a + b, self.database[tableName][column])
            elements = len(self.database[tableName][column])
            return (summ / elements)
        else:
            raise NotImplementedError(str(fun) + " function is not implemented in Mini SQL")
    
    @staticmethod
    def condition(first, second, operator):
        """
        checks the condition
        """
        if operator == '=':
            return first == second
        elif operator == '<':
            return first < second
        elif operator == '>':
            return first > second
        elif operator == '>=':
            return first >= second
        elif operator == '<=':
            return first <= second
        else:
            raise NotImplementedError(str(operator) + " is not implemented in Mini SQL")
    
    def joinHelper(self, tableList, ind, rowList):
        """
        Recursive function for joining tables
        """
        if ind == len(tableList):
            pass
        table = tableList[ind]
        colName = self.tableInfo[table][0]
        for i in range(len(self.database[table][colName])):
            rowList.append(i)
            self.joinHelper(tableList, ind+1, rowList)
            rowList.pop()

    def joinTables(self, tableList):
        """
        Cartesian product tables in tableList 
        """
        for tableName in tableList:
            if tableName not in self.tableInfo.keys():
                raise FileNotFoundError(str(tableName) + " table does not exist in the database")
        
        if len(tableList) == 1:
            return self.database[tableList[0]]
        table = OrderedDict()


    def distinct(self, tableList, columnList):
        """
        SELECT DISTINCT col1, col2, .... FROM table1,table2, .... WHERE condition
        We receive a list of table names, first we need to get a single table by joining them.
        """
        pass



