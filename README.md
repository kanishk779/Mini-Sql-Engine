# Mini-Sql-Engine
Mini Sql engine which runs a subset of queries using the command line interface.

### Dataset 
1. CSV (comma separated values) format file is provided as the dataset which is to
be used for answering sql queries.
2. All the elements in the file will be integers.
3. metadata.txt file is provided as schema of the table.
4. Column names are unique among all the tables.

### Types of queries
1. **Project** : projection operation in relational algebra.
2. **Aggregate Functions** : simple functions on single column, such as max,
   min, avg, count.
3. **Distinct** : delta operator in relational algebra.
4. **Where** : conditions with maximum of one **OR** , **AND** .
5. **Group By** : grouping of results by a single column.
6. **Order By** : order the result in ascending or descending, by a single
   column.

### MySQLParser
1. Custom sql parser is created to parse the query and create a ordered
   dictionary which can be used to execute the query.
2. First the query is checked for its correctness, if it is not, then a proper
   error message is displayed.
3. The query is separated into various parts using the **sqlparse** library.
4. Using the above created list of lists, we fill the dictionary with
   information regarding each part.


### Mini SQL Engine Class
1. get meta information.
2. fill the contents in the tables/database.
3. A good design decision that i took was to have the table passed around to each
of the methods.
4. Join helper is the recursive function for joining the tables. A temporary
ordered dict is maintained for joining the tables (cartesian product).
5. We need to give names to the newly created columns, which will be like
COUNT(col1), MAX(col2), etc.
6. The conditions are handled by the custom filter method, if there are two
conditions joined by an **AND** or an **OR** we need to apply these conditions
on the two tables obtained.
7. Show output method is used for pretty printing the resulting table, after the
query.
8. Order by is performed using the in-built sort function.


