import sys
import os
import csv
import re
import copy
import sqlparse
import functools
from collections import OrderedDict

class MiniSQL:
    def __init__(self):
        self.tableInfo = OrderedDict()
        self.database = OrderedDict() # database[TABLE_NAME][COLUMN_NAME] -> gives list of values in this column
        self.joinT = OrderedDict()
        self.getMetaInfo()
        self.fillContent()
    
    def getMetaInfo(self):
        """
        Reads the metadata.txt file to read in the schema of the database.
        Each table in database is stored in 
        """
        try:
            infoFile = open('metadata.txt', 'r')
        except FileNotFoundError as e:
            print("metadata.txt not found")
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

    def aggregate(self, table, column, fun, column2=None, valu=None):
        """
        Gives the aggregate function 'fun' on 'tableName' for 'column'
        args : tableName -> name of the table on which we need to aggregate
                column -> column used
        NOTE We will have group by based on just one column, hence this simple implementation works
        """
        if column not in table.keys():
            raise NotImplementedError("Table does not have any column named " + str(column))
        
        if column2 != None and column2 not in table.keys():
            raise NotImplementedError("Table does not have any column named " + str(column))

        if fun == 'MAX':
            val = int(-1e9)
            i = 0
            for v in table[column]:
                if column2 != None:
                    if table[column2][i] == valu:
                        val = max(val, v)
                else:
                    val = max(val, v)
                i += 1
            return val
        elif fun == 'MIN':
            val = int(1e9)
            i = 0
            for v in table[column]:
                if column2 != None:
                    if table[column2][i] == valu:
                        val = min(val, v)
                else:
                    val = min(val, v)
                i += 1
            return val
        elif fun == 'COUNT':
            if column2 != None:
                i = 0
                for v in table[column2]:
                    if v == valu:
                        i += 1
                return i
            else:
                return len(table[column])
        elif fun == 'SUM':
            if column2 != None:
                s = 0
                i = 0
                for v in table[column]:
                    if table[column2][i] == valu:
                        s += v
                    i += 1
                return s
            else:
                return functools.reduce(lambda a,b : a + b, table[column])
        elif fun == 'AVG':
            summ = 0
            elements = 0
            if column2 != None:
                i = 0
                for v in table[column]:
                    if table[column2][i] == valu:
                        summ += v
                        elements += 1
                    i += 1
            else:
                summ = functools.reduce(lambda a,b : a + b, table[column])
                elements = len(table[column])
            return (summ / elements)
        else:
            raise NotImplementedError(str(fun) + " function is not implemented in Mini SQL")
    
    @staticmethod
    def condition(first, second, operator):
        """
        checks the condition
        args : first,second -> int operands
                operator -> binary function
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
        args : tableList -> list of tables to be joined (list of strings)
                ind -> index of current table that is being processed (int)
                rowList -> list of indices of rows of various tables (list of int)
        """
        if ind == len(tableList):
            for i in range(ind):
                row = rowList[i]
                tableP = tableList[i]
                for col in self.tableInfo[tableP]:
                    self.joinT[col].append(self.database[tableP][col][row])

        table = tableList[ind]
        colName = self.tableInfo[table][0]
        for i in range(len(self.database[table][colName])):
            rowList.append(i)
            self.joinHelper(tableList, ind+1, rowList)
            rowList.pop()

    def joinTables(self, tableList):
        """
        Cartesian product tables in tableList
        args : tableList -> list of tables to be joined (list of strings)
        """
        for tableName in tableList:
            if tableName not in self.tableInfo.keys():
                raise FileNotFoundError(str(tableName) + " table does not exist in the database")
        
        if len(tableList) == 1:
            return self.database[tableList[0]]
        self.joinT = OrderedDict()
        for i in range(len(tableList)):
            for col in self.tableInfo[tableList[i]]:
                joinT[col] = []
        rowList = []
        self.joinHelper(tableList, 0, rowList)
        return self.joinT

    def project(self, columnList, table):
        """
        Projection in SQL
        args : columnList -> list of columns to be project (list of strings)
                table -> Relation on which projection has to be applied (dictionary, in column form)
        """
        result = OrderedDict()
        for column in columnList:
            if(column not in table.keys()):
                raise NotImplementedError(str(column) + " column does not exist in this table (projection)")
            result[column] = table[column]
        return result
    
    @staticmethod
    def rowForm(table):
        """
        converts table into row form from column form
        args : table -> Relation
        """
        rowTable = []
        first = True
        for key, value in table:
            if first:
                for j in range(len(value)):
                    rowTable.append([])
                first = False
            i = 0
            for val in value:
                rowTable[i].append(val)
                i += 1
        return rowTable

    def distinct(self, table):
        """
        SELECT DISTINCT col1, col2, .... FROM table1,table2, .... WHERE condition
        We receive a list of table names, first we need to get a single table by joining them.
        args : table -> Relation
        returns distinct table in row form
        """
        tupleset = set()
        rowTable = self.rowForm(table)
        
        for row in rowTable:
            tupleset.add(row)
        result = []
        for t in tupleset:
            result.append(t)
        return result
    
    @staticmethod
    def newCols(colOP):
        cols = OrderedDict()
        for key, val in colOP:
            val = val.upper()
            cols[key] = val + "(" + key + ")"
        return cols

    def groupBy(self, table, column, colOP):
        """
        args : table -> Relation
                column -> on which we need to group by
                colOP -> a dictionary which maps cols to aggregate functions
        We need to give names to the newly created columns, which will be like COUNT(col1), MAX(col2), etc
        """
        cols = newCols(colOP)
        i = 0
        seen = OrderedDict()
        newTable = OrderedDict()
        for key, val in cols:
            newTable[val] = []
        newTable[column] = []

        if column not in table.keys():
            raise NotImplementedError(str(column) + " column does not exist in this table (projection)")
        for v in table[column]:
            if v not in seen.keys():
                seen[v] = 1
                newTable[column].append(v)
                for key, val in cols:
                    res = self.aggregate(table, key, colOP[key], column, v)
                    newTable[val].append(res)

        return newTable

    def filterHelper(self, table, col1, col2, operator):
        """
        This method removes rows(tuples) which do not satisfy the condition
        return a list of row indices which should be preserved in the resulting table
        args : table -> Relation
                col1 -> first operand
                col2 -> second operand
        """
        rowTable = rowForm(table)
        result = []
        for i in range(len(table[col1])):
            if condition(table[col1][i], table[col2][i], operator):
                result.append(i)
        return result

    def customFilter(self, table, whereCond):
        """
        It modifies the table according to the where condition, there can be only atmost one 'AND' or 'OR'
        args : table -> Relation
                whereCond -> condition to be satisfied tuple of three values (column, (column or constant value), operator)
        """
        whereCond = whereCond.strip()


class MySQLParser:
    def __init__(self, query):
        self.query = query
        self.info = OrderedDict()
        self.info["columns"] = []
        self.info["tables"] = []
        self.info["distinct"] = False
        self.info["groupby"] = [] # there will be atmost one column
        self.info["orderby"] = [] # there will be atmost one column
        self.info["conditions"] = [] # atmost 2 conds, each cond is tuple of (first, second, op), first is a column, second can be a column or constant value
        self.info["between_cond_op"] = ""
    
    def parser(self):
        keywords = self.separator()
        self.fillDict(keywords)
        return self.info

    def separator(self):
        raw = copy.deepcopy(self.query)
        raw = sqlparse.format(raw, reindent=True, keyword_case='upper')
        parsed = sqlparse.parse(raw)
        parsed = parsed[0]
        keywords = []
        for i in range(len(parsed.tokens)):
            if str(parsed.tokens[i]) != ' ':
                keywords.append(str(parsed.tokens[i]).strip('\n\r '))
    
    def fillDict(self, keywords):
        From = False
        group = False
        order = False
        dist = False
        for s in keywords:
            s = s.strip()
            s = s.split(' ')
            if s[0] == '' or s[0] == ' ':
                continue
            temp = []
            # remove any space or empty strings
            for val in s:
                if val != '' and val != ' ':
                    temp.append(val)
            s = temp
            if "WHERE" in s:
                if "AND" in s or "OR" in s:
                    self.info["between_cond_op"] = s[4]
                    ss = ""
                    for k in s[3]:
                        if k != '\n':
                            ss += str(k)
                    cond1 = (s[1], ss, s[2])
                    cond2 = (s[5], s[7], s[6])
                    self.info["conditions"].append(cond1)
                    self.info["conditions"].append(cond2)
                else:
                    cond = (s[1], s[3], s[2])
                    self.info["conditions"].append(cond)
            if "GROUP" in s:
                group = True
                continue
            if "ORDER" in s:
                order = True
                continue
            if "FROM" in s:
                From = True
                continue
            if "DISTINCT" in s:
                self.info["distinct"] = True
                continue
            if From:
                From = False
                for tab in s:
                    self.info["tables"].append(str(tab))
            elif order:
                order = False
                for col in s:
                    self.info["orderby"].append(str(col))
            elif group:
                group = False
                for col in s:
                    self.info["groupby"].append(str(col)) 

        colList = ""
        if self.info["distinct"]:
            colList = keywords[2]
        else:
            colList = keywords[1]
        colList = colList.strip()
        colList = colList.split(' ')
        temp = []
        # remove any space or empty strings
        for val in colList:
            if val != '' and val != ' ':
                temp.append(val)
        colList = temp
        for column in colList:
            self.info["columns"].append(column)