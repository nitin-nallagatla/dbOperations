import logging

import mysql.connector
import pandas as pd
import pymysql
import sqlalchemy
from mysql.connector import Error
from pandas import DataFrame
from sqlalchemy import create_engine


class mySql:
    try:

        logging.basicConfig(filename='mySqlClass.log', filemode='w',
                            format='[{%(asctime)s} %(filename)s: %(lineno)d]\t%(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)

        def connectMethod(self, username, password, tableName, usernameCol, passwordCol, cursor):
            """
            Checks to see if username and password are in table.
            Parameters:
                username (String) : Username to search for in table.
                password (String) : Password to search for in table.
                tableName (String) : Name of table to search in.
                usernameCol (String) : Name of column to look for username in.
                passwordCol (String) : Name of column to look for password in.
                cursor (mySql Connection) : Cursor to execute mySql queries with.
            Returns:
                Returns true or false based on whether username and table are in the table. Logs results.
            """
            sql = "SELECT * FROM " + tableName + " where " + usernameCol + " = '" + \
                  username + "' and " + passwordCol + " = " + str(password) + ")"

            sqlQuery = "SELECT EXISTS(" + sql

            try:
                cursor.execute(sqlQuery)
                num = cursor.fetchall()

                if list(num).pop()[0] == 1:
                    logging.info("Matched Credentials")
                    return True
                else:
                    logging.info("Failed to Match Credentials")
                    return False
            except Error as e:
                logging.error("connectMethod: {}".format(e))
                return -1

        def readTableData(self, tableName, conn):
            """
            Reads all table data and returns as dataframe.
            Parameters:
                tableName (String) : Name of table to read data from.
                conn (mySql) : Connection to copy table data into dataframe.
            Returns:
                Returns dataframe made out of table or -1 if error occurs. Logs success or failure, and prints dataframe.
            """
            sql = "Select * from %s" % tableName
            try:
                df = pd.read_sql(sql, conn)
                logging.info("Successfully returned dataframe.")
                logging.info("readTableData: {}".format(df))
                return df
            except mysql.connector.errors.ProgrammingError as e:
                logging.error("readTableData: {}".format(e))
                return None
            except pd.io.sql.DatabaseError as e:
                logging.error("readTableData: {}".format(e))
                return None
            except Error as e:
                logging.error("readTableData: {}".format(e))
                return None
            finally:
                conn.close()

        def readSampleData(self, numRows, tableName, cursor):
            """
            Returns random amount of sample rows from a table.
            Parameters:
                numRows (Integer) : Number of random rows to read from table.
                tableName (String) : Name of table to take rows from.
                cursor (mySql Connection) : Cursor to execute mySql queries with.
            Returns:
                Returns random rows as a list of sets. Logs success or failure and prints results.
            """
            sql = "SELECT * FROM " + tableName + " Order by Rand() Limit " + str(numRows) + ";"
            # print(sql)
            try:
                cursor.execute(sql)
                result = cursor.fetchall()
                logging.info("Successfully performed query.")
                logging.info(result)
                return result
            except Error as e:
                logging.error("readSampleData: {}".format(e))
                return None

        def readColumnData(self, tableName, columnName, cursor):
            """
            Displays contents of a column in table and returns table details as dataframe.
            Parameters:
                tableName (String) : Name of table to find column in.
                columnName (String) : Name of column to display data from and get specifications of.
                cursor (mySql Connection) : Cursor to execute mySql queries with.
            Returns:
                Returns dataframe with the specifications and prints out data. Logs success or failure and prints out results.
            """
            try:
                cursor.execute("Select " + columnName + " from " + tableName)
                result = cursor.fetchall()
                cursor.execute("SHOW COLUMNS FROM " + tableName + " where Field in('%s')" % columnName)
                statistics = cursor.fetchall()
                details = []
                info = []

                for i in result:
                    info.append(i[0])

                logging.info(info)

                for j in statistics[0]:
                    details.append(j)

                # conclusiveList = [details, content]
                df = DataFrame(details, columns=['Specifications'])
                logging.info("Successfully returned Data Frame.")
                logging.info("readColumnData: {}".format(df))

                return df
            except Error as e:
                logging.error("readColumnData: {}".format(e))
                return None

        def findRow(self, searchDict, tableName, cursor):
            """
            Checks to see if row matching details in dictionary are in table.
            Parameters:
                searchDict (Dictionary) : Dictionary to use to search for row.
                tableName (String) : Name of table to search for row in.
                cursor (mySql Connection) : Cursor to execute mySql queries with.
            Returns:
                Returns contents of the row, None if the row is empty, or -1 if an error occurred. Logs success or failure.
            """
            sql = "select * from " + tableName + " where "
            for key in list(searchDict)[:-1]:
                sql += key + " = '%s'  and " % str(searchDict[key])
            sql += (list(searchDict.items())[-1])[0] + " = '%s'; " % str((list(searchDict.items())[-1])[1])
            try:
                cursor.execute(sql)
                result = cursor.fetchall()

                if (len(result) is 0):
                    logging.info("No match.")
                    return None
                logging.info("Match Found.")
                logging.info(result)
                return result

            except Error as e:
                logging.error("findRow: {}".format(e))
                return None

        def loadData(self, df, tableName):
            """
            Creates table with data from dataframe, or appends to existing table.
            Parameters:
                tableName (String) : Table to append to or create to.
                df (DataFrame) : Pandas DataFrame to get data from.
            Returns:
                Does not return anything, creates or fills table. Logs success or failure.
            """
            try:
                my_conn = create_engine('mysql+pymysql://test:password@localhost/sampleDatabase')
                df.to_sql(con=my_conn, name=tableName, if_exists='append', index=False)
                logging.info(
                    "Successfully created table or appended data to existing table with same name and format. ")
                return None
            except pymysql.err.OperationalError as e:
                logging.info("loadData: {}".format(e))
                return e
            except sqlalchemy.exc.OperationalError as e:
                logging.info("loadData: {}".format(e))
                return e
            except Error as e:
                logging.info("loadData: {}".format(e))
                return e

        def createObject(self, cursor, tableName, columnName):
            """
            Creates table out of the contents of a single column in given table.
            Parameters:
                tableName (String) : Name of table to search for column in.
                columnName (String) : Name of column to duplicate and convert into new table.
                cursor (mySql Connection) : Cursor to execute mySql queries with.
            Returns:
                Returns 1 if successful or None if there was an error. Logs success or failure.
            """
            try:
                specs = mySql.readColumnData(self, tableName, columnName, cursor)

                if specs is not None:
                    listOfSpecs = specs.values.tolist()
                else:
                    logging.error("Table or Column Not Found. ")
                    return None

                sql = "create table " + columnName + " (" + listOfSpecs[0][0] + " " + listOfSpecs[1][0].decode(
                    "utf-8") + "); "
                cursor.execute(sql)
                logging.info("Successfully created table")
                sql = "show tables;"

                cursor.execute(sql)
                listOfNames = []
                for row in cursor:
                    listOfNames.append(row)

                logging.info(listOfNames)
                return 1

            except Error as e:
                logging.info("createObject: {}".format(e))
                return None

        def dropTable(self, tableName, cursor):
            """
            Drops table.
            Parameters:
                tableName (String) : Name of table to delete.
                cursor (mySql Connection) : Cursor to execute mySql queries with.
            Returns:
                Returns nothing if successful or -1 if there was an error. Logs tables before and after execution, and logs number of tables. Logs success or failure as well.
            """
            listOfTables = []
            try:
                logging.info("Before Execution: ")
                cursor.execute("Show tables;")
                i = 0

                for row in cursor:
                    i += 1
                    listOfTables.append(row[0])

                info = "Number of Tables: %d" % i
                logging.info(info)
                logging.info(listOfTables)
                sql = "drop table " + tableName
                cursor.execute(sql)

                logging.info("Table dropped. ")
                cursor.execute("show tables; ")
                i = 0
                listOfTables = []

                logging.info("After Execution: ")
                for row in cursor:
                    listOfTables.append(row[0])
                    i += 1

                info = "Number of Tables: %d" % i
                logging.info(info)
                logging.info(listOfTables)

            except Error as e:
                logging.info("dropTable: {}".format(e))
                return -1

        def readQueryData(self, statement, conn):
            """
            Executes query and returns as Data Frame.
            Parameters:
                statement (String) : Statement to execute and convert to dataframe.
                conn (mySql) : Connection to create dataframe with.
            Returns:
                Returns dataframe that occurs as result of query. Logs success or failure and prints data frame.
            """
            try:
                df = pd.read_sql(statement, conn)
                logging.info("Successfully created Data Frame")
                if df is not None:
                    logging.info("readQueryData: {}".format(df))
                    return df
                else:
                    logging.info("DataFrame is empty")
                    return df
            except mysql.connector.errors.ProgrammingError as e:
                logging.error("readTableData: {}".format(e))
                return None
            except pd.io.sql.DatabaseError as e:
                logging.error("readTableData: {}".format(e))
                return None
            except Error as e:
                logging.error("readQueryData: {}".format(e))
                return None
            # finally:
            #     conn.close()

    except Error as e:
        logging.error("Generic Error: {}".format(e))

    # finally:
    #     logging.info("ENDING")
    #     conn.close()
