import os
import copy
import sqlparse
import functools
import sys
from collections import OrderedDict


class MiniSQL:
    def __init__(self):
        self.tableInfo = OrderedDict()
        self.database = OrderedDict()  # database[TABLE_NAME][COLUMN_NAME] -> gives list of values in this column
        self.joinT = OrderedDict()
        self.get_meta_info()
        self.fill_content()

    def get_meta_info(self):
        """
        Reads the metadata.txt file to read in the schema of the database.
        Each table in database is stored in 
        """
        try:
            info_file = open('metadata.txt', 'r')
        except FileNotFoundError:
            print("metadata.txt not found")
        else:
            table_started = False
            table_name = ""
            for ro in info_file:
                if ro.strip() == '<begin_table>':
                    table_started = True
                    continue
                if ro.strip() == '<end_table>':
                    continue
                if table_started:
                    table_started = False
                    table_name = ro.strip()
                    self.tableInfo[table_name] = []
                    continue
                # append the column names into the table dict
                self.tableInfo[table_name].append(ro.strip())

    def print_database(self):
        for key, val in self.tableInfo.items():
            print(key)
            for col in val:
                print(col)
            print("---------")

    @staticmethod
    def get_csv(name):
        """
        Provides the content of the CSV file
        """
        try:
            tab_file = open(name, 'r')
        except Exception as e:
            print(name + " file does not exist")
            raise FileNotFoundError
        else:
            content = []
            for row in tab_file:
                content.append(row.strip("\n"))
            return content

    def fill_content(self):
        """
        Fills in the content in the database
        """
        # First delete those tableInfo entries whose corresponding files are not present
        to_be_remove = []
        for table in self.tableInfo.keys():
            if not os.path.isfile(str(table) + ".csv"):
                to_be_remove.append(table)
        for table in to_be_remove:
            print(table)
            del self.tableInfo[table]

        # Initialise the database
        for table in self.tableInfo.keys():
            self.database[table] = OrderedDict()
            for column in self.tableInfo[table]:
                self.database[table][column] = []  # Each column has a list of data

        # Finally fill the content in database
        for table in self.tableInfo.keys():
            rows = MiniSQL.get_csv(str(table) + ".csv")
            for row in rows:
                data = row.split(',')
                for i in range(len(data)):
                    col_name = self.tableInfo[table][i]
                    d = data[i].strip()
                    d = d.strip('\n')
                    self.database[table][col_name].append(int(d))

    def aggregate(self, table, column, fun, grouped_column=None, valu=None):
        """
        Gives the aggregate function 'fun' on 'table' for 'column'
        args : tableName -> name of the table on which we need to aggregate
                column -> column used
                fun -> function applied
        NOTE We will have group by based on just one column, hence this simple implementation works
        """
        if column == '*':
            column = next(iter(table))  # this takes care of COUNT(*), because we can safely replace column with
            # first key i.e a column of table here
        if column not in table.keys():
            raise NotImplementedError("Table does not have any column named " + str(column))

        if grouped_column is not None and grouped_column not in table.keys():
            raise NotImplementedError("Table does not have any column named " + str(column))

        if fun == 'MAX':
            val = int(-1e9)
            i = 0
            for v in table[column]:
                if grouped_column is not None:
                    if table[grouped_column][i] == valu:
                        val = max(val, v)
                else:
                    val = max(val, v)
                i += 1
            return val
        elif fun == 'MIN':
            val = int(1e9)
            i = 0
            for v in table[column]:
                if grouped_column is not None:
                    if table[grouped_column][i] == valu:
                        val = min(val, v)
                else:
                    val = min(val, v)
                i += 1
            return val
        elif fun == 'COUNT':
            if grouped_column is not None:
                i = 0
                for v in table[grouped_column]:
                    if v == valu:
                        i += 1
                return i
            else:
                return len(table[column])
        elif fun == 'SUM':
            if grouped_column is not None:
                s = 0
                i = 0
                for v in table[column]:
                    if table[grouped_column][i] == valu:
                        s += v
                    i += 1
                return s
            else:
                return functools.reduce(lambda a, b: a + b, table[column])
        elif fun == 'AVG':
            summ = 0
            elements = 0
            if grouped_column is not None:
                i = 0
                for v in table[column]:
                    if table[grouped_column][i] == valu:
                        summ += v
                        elements += 1
                    i += 1
            else:
                summ = functools.reduce(lambda a, b: a + b, table[column])
                elements = len(table[column])
            return summ / elements
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

    def join_helper(self, table_list, ind, row_list):
        """
        Recursive function for joining tables
        args : table_list -> list of tables to be joined (list of strings)
                ind -> index of current table that is being processed (int)
                row_list -> list of indices of rows of various tables (list of int)
        """
        if ind == len(table_list):
            for i in range(ind):
                row = row_list[i]
                tableP = table_list[i]
                for col in self.tableInfo[tableP]:
                    self.joinT[col].append(self.database[tableP][col][row])
            return

        table = table_list[ind]
        col_name = self.tableInfo[table][0]
        for i in range(len(self.database[table][col_name])):
            row_list.append(i)
            self.join_helper(table_list, ind + 1, row_list)
            row_list.pop()

    def join_tables(self, table_list):
        """
        Cartesian product tables in table_list
        args : table_list -> list of tables to be joined (list of strings)
        """
        for tableName in table_list:
            if tableName not in self.tableInfo.keys():
                raise FileNotFoundError(str(tableName) + " table does not exist in the database")

        if len(table_list) == 1:
            return self.database[table_list[0]]
        self.joinT = OrderedDict()
        for i in range(len(table_list)):
            for col in self.tableInfo[table_list[i]]:
                self.joinT[col] = []
        row_list = []
        self.join_helper(table_list, 0, row_list)
        return self.joinT

    def project(self, table, column_list):
        """
        Projection in SQL
        args : table -> Relation on which projection has to be applied (dictionary, in column form)
                column_list -> list of columns to be project (list of strings)
        """
        result = OrderedDict()
        if len(column_list) == 1 and column_list[0] == '*':
            return table
        for column in column_list:
            if column not in table.keys():
                raise NotImplementedError(str(column) + " column does not exist in this table (projection)")
            result[column] = table[column]
        return result

    @staticmethod
    def row_form(table):
        """
        converts table into row form from column form
        args : table -> Relation
        """
        row_table = []
        first = True
        headings = []
        for key, value in table.items():
            headings.append(key)
            if first:
                for j in range(len(value)):
                    row_table.append([])
                first = False
            i = 0
            for val in value:
                row_table[i].append(val)
                i += 1
        return row_table, headings

    @staticmethod
    def distinct(table):
        """
        We receive a list of table names, first we need to get a single table by joining them.
        args : table -> Relation
        returns distinct table in "ROW form" list of tuples
        """
        tupleset = OrderedDict()  # keeps the order intact
        row_table, headings = MiniSQL.row_form(table)

        for row in row_table:
            tupleset[tuple(row)] = 1
        result = []
        for key, val in tupleset.items():
            result.append(key)
        return result, headings

    @staticmethod
    def new_cols(colOP):
        cols = OrderedDict()
        for key, val in colOP.items():
            val = val.upper()
            cols[key] = val + "(" + key + ")"
        return cols

    def group_by(self, table, column, col_operation):
        """
        args : table -> Relation
                column -> on which we need to group by
                col_operation -> a dictionary which maps cols to aggregate functions
        We need to give names to the newly created columns, which will be like COUNT(col1), MAX(col2), etc
        """
        cols = MiniSQL.new_cols(col_operation)
        seen = OrderedDict()
        new_table = OrderedDict()
        for key, val in cols.items():
            new_table[val] = []
        new_table[column] = []

        if column not in table.keys():
            raise NotImplementedError(str(column) + " column does not exist in this table (projection)")
        for v in table[column]:
            if v not in seen.keys():
                seen[v] = 1
                new_table[column].append(v)
                for key, val in cols.items():
                    res = self.aggregate(table, key, col_operation[key], column, v)
                    new_table[val].append(res)

        return new_table

    @staticmethod
    def filter_helper(table, col1, col2, operator, val=None):
        """
        This method removes rows(tuples) which do not satisfy the condition
        return a list of row indices which should be preserved in the resulting table
        args : table -> Relation
                col1 -> first operand
                col2 -> second operand (can be a column or a constant value)
        """
        result = []
        for i in range(len(table[col1])):
            if val is not None:
                if MiniSQL.condition(table[col1][i], int(val), operator):
                    result.append(i)
            else:
                if MiniSQL.condition(table[col1][i], table[col2][i], operator):
                    result.append(i)
        return result

    def custom_filter(self, table, where_cond):
        """
        It modifies the table according to the where condition, there can be only atmost one 'AND' or 'OR'
        args : table -> Relation
                where_cond -> condition to be satisfied tuple of three values (column, (column or constant value), operator)
        """
        const_val = True
        for char in where_cond[1]:
            if not ('0' <= char <= '9'):
                const_val = False
        if const_val:
            return self.filter_helper(table, where_cond[0], "Does'nt Matter", where_cond[2], int(where_cond[1]))
        else:
            return self.filter_helper(table, where_cond[0], where_cond[1], where_cond[2])

    def where(self, table, conditions, op=None):
        """
        Returns the table after filtering it based on the supplied conditions
        args : table -> Relation
                conditions -> conditions to be applied
        """
        # IF there is just one condition
        rows_to_keep = []
        if len(conditions) == 1:
            rows_to_keep = self.custom_filter(table, conditions[0])
        else:
            # IF there are two conditions
            row1 = self.custom_filter(table, conditions[0])
            row2 = self.custom_filter(table, conditions[1])
            some_column = conditions[0][0]
            total = len(table[some_column])
            if op == "OR":
                for i in range(total):
                    if i in row1 or i in row2:
                        rows_to_keep.append(i)
            elif op == "AND":
                for i in range(total):
                    if i in row1 and i in row2:
                        rows_to_keep.append(i)
            else:
                raise NotImplementedError("Invalid where condition (syntax error)")
        new_table = OrderedDict()
        for key, val in table.items():
            new_table[key] = []
            for i in rows_to_keep:
                new_table[key].append(table[key][i])
        return new_table

    @staticmethod
    def order_by(table, column, sorting_type):
        """
        Returns the table after sorting it based on the column
        args : table -> Relation
                column -> column based on which we want to sort
        """
        new_tuple = []  # list of tuples
        i = 0
        for val in table[column]:
            new_tuple.append((val, i))
            i += 1
        if sorting_type == "ASC":
            new_tuple.sort(key=lambda x: x[0])
        else:
            new_tuple.sort(key=lambda x: x[0], reverse=True)
        new_table = OrderedDict()
        # table is stored in form of dictionary, key is column name and val is list of values
        for key, col in table.items():
            new_table[key] = []
            for tt in new_tuple:
                ind = tt[1]  # index which is to be next appended
                new_table[key].append(col[ind])
        return new_table

    @staticmethod
    def show_output(table, headings=None):
        """
        Prints the table for output
        """
        sep = "-----------------"
        sep = len(table) * sep
        if isinstance(table, OrderedDict):
            print(sep)
            for key in table.keys():
                print(key, end="\t")
            print()
        else:
            for key in headings:
                print(key, end="\t")
            print()
        print(sep)
        new_table = table
        if isinstance(table, OrderedDict):
            new_table, _ = MiniSQL.row_form(table)

        for row in new_table:
            for entry in row:
                print(entry, end="\t\t")
            print()
        print(sep)


class MySQLParser:
    def __init__(self, query):
        self.query = query
        self.info = OrderedDict()
        self.info["columns"] = []
        self.info["tables"] = []
        self.info["groupby"] = []  # there will be atmost one column
        self.info["orderby"] = []  # there will be atmost one column
        self.info["conditions"] = []  # Atmost 2 conditions, each condition is tuple of (first, second, op), first is
        # a column, second can be a column or constant value
        self.info["between_cond_op"] = ""
        self.info["orderbytype"] = "ASC"
        self.info["hasgroupby"] = False
        self.info["hasorderby"] = False
        self.info["distinct"] = False
        self.info["where"] = False

    def parse(self):
        """
        parses the sql query and raise exceptions if it is not correct syntactically
        """
        if self.query[-1] != ';':
            raise NotImplementedError("Semicolon missing")
        self.query = self.query[:-1]
        keywords = self.separator()
        self.fill_dict(keywords)
        if len(self.info["tables"]) == 0:
            raise NotImplementedError("Syntax error in SQL query, no tables mentioned in query")
        if len(self.info["columns"]) == 0:
            raise NotImplementedError("Syntax error in SQL query, no columns or aggregation mentioned to be selcted")
        if self.info["hasgroupby"] and len(self.info["groupby"]) != 1:
            raise NotImplementedError("Syntax error in SQL query, we exactly support one column for GROUP BY")
        if self.info["hasorderby"] and len(self.info["orderby"]) != 1:
            if len(self.info["orderby"]) > 2 or (
                    len(self.info["orderby"]) == 2 and self.info["orderby"][1] != "ASC" and self.info["orderby"][
                1] != "DESC"):
                raise NotImplementedError("Syntax error in SQL query, we exactly support one column for ORDER BY")
            else:
                self.info["orderbytype"] = self.info["orderby"][1]
                temp = [self.info["orderby"][0]]
                self.info["orderby"] = temp
        if self.info["distinct"] and (
                len(self.info["orderby"]) > 0 and self.info["orderby"][0] not in self.info["columns"]):
            raise NotImplementedError(
                "Syntax error in SQL query, DISTINCT used and ORDER BY uses columns not mentioned in SELECT")

        return self.info

    def print_parse_info(self):
        for key, val in self.info.items():
            print(key, end=" : ")
            print(val)
    
    def separator(self):
        """
        separates the query to list of list where each sublist is one part of the query, example WHERE clause.
        it returns the list of list
        """
        raw = copy.deepcopy(self.query)
        raw = sqlparse.format(raw, reindent=True, keyword_case='upper')
        parsed = sqlparse.parse(raw)
        parsed = parsed[0]
        keywords = []
        for i in range(len(parsed.tokens)):
            if str(parsed.tokens[i]) != ' ':
                keywords.append(str(parsed.tokens[i]).strip('\n\r '))
        new_keywords = []
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
            new_keywords.append(temp)
        if len(new_keywords) < 4:
            raise NotImplementedError("Syntax error in SQL query, very short incomplete query")
        return new_keywords

    def fill_dict(self, keywords):
        """
        Fills the dictionary with information regarding the query
        args : keywords -> separate query
        """
        tab_start = False
        group = False
        order = False
        for s in keywords:
            if "WHERE" in s:
                if len(s) < 4:
                    raise NotImplementedError("Syntax error in WHERE clause, condition not mentioned properly")
                self.info["where"] = True
                if "AND" in s or "OR" in s or len(s) > 4:
                    # if some invalid between condition is present like NAND, it will be handled in where function in
                    # MiniSQL class 
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
                order = False
                tab_start = False
                self.info["hasgroupby"] = True
                continue
            if "ORDER" in s:
                order = True
                group = False
                tab_start = False
                self.info["hasorderby"] = True
                continue
            if "FROM" in s:
                tab_start = True
                order = False
                group = False
                continue
            if "DISTINCT" in s:
                self.info["distinct"] = True
                continue
            if tab_start:
                tab_start = False
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

        if self.info["distinct"]:
            col_list = keywords[2]
        else:
            col_list = keywords[1]
        temp = []
        # remove any space or empty strings
        for val in col_list:
            if val != '' and val != ' ':
                temp.append(val)
        col_list = temp
        for column in col_list:
            self.info["columns"].append(column)


def main():
    minisql = MiniSQL()
    keep = True
    # print("Please print all the aggregate functions in capital like COUNT, etc, wherever it is used. And don't use "
    #       "comma in the query")
    while keep:
        # query = input("mini$> ")
        query = sys.argv[1]
        if query.upper() == "QUIT":
            print("Ok")
            keep = False
        else:
            query = query.strip()
            my_parser = MySQLParser(query)
            info = my_parser.parse()
            col_op = OrderedDict()
            group_by_first = False
            # extract the column-operation dictionary using "SELECTED" columns
            for col in info["columns"]:
                aggregate = False
                cc: str = ""
                for char in col:
                    if char == ')':
                        aggregate = False
                    if aggregate:
                        cc += str(char)
                    if char == '(':
                        aggregate = True
                if "" != cc:
                    if col[0] == 'C':
                        col_op[cc] = "COUNT"
                    else:
                        col_op[cc] = str(col[:3])  # for min, max, sum, avg
            if info["hasgroupby"]:
                if len(col_op) == 0:
                    pass
                grouped_column = info["groupby"][0]
                if grouped_column in info["columns"]:
                    if (1 + len(col_op)) != len(info["columns"]):
                        raise NotImplementedError(
                            "SELECTED columns should have all the other columns as some aggregation, except the "
                            "grouped column")
                else:
                    if len(col_op) != len(info["columns"]):
                        print(len(col_op))
                        print(len(info["columns"]))
                        raise NotImplementedError(
                            "SELECTED columns should have all the columns as some aggregation")
                # IF the conditions require grouping by , then we first group by else, we group by after WHERE
                if info["where"]:
                    for tup in info["conditions"]:
                        for char in tup[0]:
                            if char == '(':
                                group_by_first = True
            else:
                if len(col_op) > 1:
                    raise NotImplementedError("Only one aggregation allowed when GROUP BY is not used")

            # join the tables
            joined_table = copy.deepcopy(minisql.join_tables(info["tables"]))
            if group_by_first:
                joined_table = copy.deepcopy(minisql.group_by(joined_table, info["groupby"][0], col_op))
            # apply the where condition
            if info["where"]:
                if info['between_cond_op'] != "aman":
                    joined_table = copy.deepcopy(
                        minisql.where(joined_table, info["conditions"], info["between_cond_op"]))

                else:
                    joined_table = copy.deepcopy(minisql.where(joined_table, info["conditions"]))
            # order by and group by will use same columns (in mini sql)
            # apply group by
            if info["hasgroupby"] and not group_by_first:
                joined_table = copy.deepcopy(minisql.group_by(joined_table, info["groupby"][0], col_op))
            # apply order by
            if info["hasorderby"]:
                joined_table = copy.deepcopy(
                    minisql.order_by(joined_table, str(info["orderby"][0]), info["orderbytype"]))
            if info["hasgroupby"] == False and len(col_op) == 1:
                query_col = ""
                query_fun = ""
                for key, val in col_op.items():
                    query_col = key
                    query_fun = val
                value = minisql.aggregate(joined_table, query_col, query_fun)
                joined_table[query_fun + "(" + query_col + ")"] = [value]

            # project the columns
            joined_table = copy.deepcopy(minisql.project(joined_table, info["columns"]))
            # apply distinct
            if info["distinct"]:
                joined_table, headings = MiniSQL.distinct(joined_table)
                MiniSQL.show_output(joined_table, headings)
            else:
                MiniSQL.show_output(joined_table)
            keep = False


if __name__ == "__main__":
    main()
