import unittest

import mysql
import pandas as pd

import mySqlClass


class testCases(unittest.TestCase):
    conn = mysql.connector.connect(user='test',
                                   password='password',
                                   database='sampleDatabase')
    cursor = conn.cursor()
    msc = mySqlClass.mySql()

    def testConnectMethod(self, username="John", password=14789, tableName="student", usernameCol="name",
                          passwordCol="id"):
        """
        Positive Test Case for Connect Method.
        Parameters:
            username (String) : Username to search for in table.
            password (String) : Password to search for in table.
            tableName (String) : Name of table to search in.
            usernameCol (String) : Name of column to look for username in.
            passwordCol (String) : Name of column to look for password in.
        Returns:
             If successful, returns nothing.
        """
        self.conn.reconnect()
        assert self.msc.connectMethod(username, password, tableName, usernameCol, passwordCol,
                                      self.cursor) == True, "Unexpected Error"
        self.conn.close()

    def testConnectMethod1(self, username="John", password=14789, tableName="student", usernameCol="error",
                           passwordCol="id"):
        """
        Negative Test Case for Connect Method.
        Parameters:
            username (String) : Username to search for in table.
            password (String) : Password to search for in table.
            tableName (String) : Name of table to search in.
            usernameCol (String) : Name of column to look for username in.
            passwordCol (String) : Name of column to look for password in.
        Returns:
             Expected to fail and returns nothing if it does.
        """
        self.conn.reconnect()
        assert self.msc.connectMethod(username, password, tableName, usernameCol, passwordCol,
                                      self.cursor) == -1, "Incorrect Columns Given"
        self.conn.close()

    def testConnectMethod2(self, username="John", password=14789, tableName="error", usernameCol="name",
                           passwordCol="id"):
        """
        Negative Test Case for Connect Method.
        Parameters:
            username (String) : Username to search for in table.
            password (String) : Password to search for in table.
            tableName (String) : Name of table to search in.
            usernameCol (String) : Name of column to look for username in.
            passwordCol (String) : Name of column to look for password in.
        Returns:
             Expected to fail and returns nothing if it does.
        """
        self.conn.reconnect()
        assert self.msc.connectMethod(username, password, tableName, usernameCol, passwordCol,
                                      self.cursor) == -1, "Invalid Table Name"
        self.conn.close()

    def testConnectMethod3(self, username="John", password=00000, tableName="student", usernameCol="name",
                           passwordCol="id"):
        """
        Negative Test Case for Connect Method.
        Parameters:
            username (String) : Username to search for in table.
            password (String) : Password to search for in table.
            tableName (String) : Name of table to search in.
            usernameCol (String) : Name of column to look for username in.
            passwordCol (String) : Name of column to look for password in.
        Returns:
             Expected to return false and does nothing if it does.
        """
        self.conn.reconnect()
        assert self.msc.connectMethod(username, password, tableName, usernameCol, passwordCol,
                                      self.cursor) == False, "Invalid Values"
        self.conn.close()

    def testReadTableData(self, tableName="student"):
        """
        Positive test case for ReadTableData Method.
        Parameters:
            tableName (String) : Name of table to read data from.

        Returns:
            If successful, returns nothing.
        """
        self.conn.reconnect()
        result = self.msc.readTableData(tableName, self.conn)
        assert result.empty != True, "Unexpected Error Occurred"
        self.conn.close()

    def testReadTableData1(self, tableName="error"):
        """
        Negative test case for ReadTableData Method.
        Parameters:
            tableName (String) : Name of table to read data from.
        Returns:
            Expected to fail and returns nothing if so.
        """
        self.conn.reconnect()
        result = self.msc.readTableData(tableName, self.conn)
        self.assertIsNone(result, "Incorrect Table Name")
        self.conn.close()

    def testReadSampleData(self, numRows=3, tableName="student"):
        """
        Positive test case for Read Sample Data Method.
        Parameters:
            numRows (Integer) : Number of random rows to read from table.
            tableName (String) : Name of table to take rows from.
        Returns:
            Expected to return nothing if successful.
        """
        self.conn.reconnect()
        self.assertIsNotNone(self.msc.readSampleData(numRows, tableName, self.cursor), "Unexpected Error Occurred")
        self.conn.close()

    def testReadSampleData1(self, numRows=True, tableName="student"):
        """
        Negative test case for Read Sample Data Method.
        Parameters:
            numRows (Integer) : Number of random rows to read from table.
            tableName (String) : Name of table to take rows from.
        Returns:
            Expected to return nothing if fails as intended.
        """
        self.conn.reconnect()
        self.assertIsNone(self.msc.readSampleData(numRows, tableName, self.cursor), "Not a number")
        self.conn.close()

    def testReadColumnData(self, tableName="student", columnName="name"):
        """
        Positive Test Case for Read Column Data Method.
        Parameters:
            tableName (String) : Name of table to find column in.
            columnName (String) : Name of column to display data from and get specifications of.
        Returns:
            Supposed to return nothing if successful.
        """
        self.conn.reconnect()
        self.assertIsNotNone(self.msc.readColumnData(tableName, columnName, self.cursor), "Unexpected Error Occurred")
        self.conn.close()

    def testReadColumnData1(self, tableName="error", columnName="name"):
        """
        Negative Test Case for Read Column Data Method.
        Parameters:
            tableName (String) : Name of table to find column in.
            columnName (String) : Name of column to display data from and get specifications of.
        Returns:
            Supposed to return nothing if failed as predicted.
        """
        self.conn.reconnect()
        self.assertIsNone(self.msc.readColumnData(tableName, columnName, self.cursor), "Invalid Table Name")
        self.conn.close()

    def testReadColumnData2(self, tableName="student", columnName="error"):
        """
        Negative Test Case for Read Column Data Method.
        Parameters:
            tableName (String) : Name of table to find column in.
            columnName (String) : Name of column to display data from and get specifications of.
        Returns:
            Supposed to return nothing if failed as predicted.
        """
        self.conn.reconnect()
        self.assertIsNone(self.msc.readColumnData(tableName, columnName, self.cursor), "Invalid Column Name")
        self.conn.close()

    def testFindRow(self, searchDict={'name': 'John', 'id': 14789}, tableName="student"):
        """
        Successful test case for FindRow Method.
        Parameters:
            searchDict (Dictionary) : Dictionary to use to search for row.
            tableName (String) : Name of table to search for row in.
        Returns:
            Supposed to return nothing if successful.
        """
        self.conn.reconnect()
        self.assertIsNotNone(self.msc.findRow(searchDict, tableName, self.cursor), "Unexpected error Occurred")
        self.conn.close()

    def testFindRow1(self, searchDict={'error': 'John', 'id': 14789}, tableName="student"):
        """
        Negative test case for FindRow Method.
        Parameters:
            searchDict (Dictionary) : Dictionary to use to search for row.
            tableName (String) : Name of table to search for row in.
        Returns:
            Supposed to return nothing if failed as expected.
        """
        self.conn.reconnect()
        self.assertIsNone(self.msc.findRow(searchDict, tableName, self.cursor), "Invalid Dictionary")
        self.conn.close()

    def testFindRow2(self, searchDict={'name': 'John', 'id': 14789}, tableName="error"):
        """
        Negative test case for FindRow Method.
        Parameters:
            searchDict (Dictionary) : Dictionary to use to search for row.
            tableName (String) : Name of table to search for row in.
        Returns:
            Supposed to return nothing if failed as expected.
        """
        self.conn.reconnect()
        self.assertIsNone(self.msc.findRow(searchDict, tableName, self.cursor), "Invalid Table")
        self.conn.close()

    testingDict = {'one': [1, 1, 1], 'two': [2, 2, 2], 'three': [3, 3, 3]}

    def testLoadData(self, testingNewDf=pd.DataFrame.from_dict(testingDict), tableName="testing1"):
        """
        Positive Test Case for Load Data Method.
        Parameters:
            tableName (String) : Table to append to or create to.
            df (DataFrame) : Pandas DataFrame to get data from.
        Returns:
            Returns nothing if successful.
        """
        self.conn.reconnect()
        self.assertIsNone(self.msc.loadData(testingNewDf, tableName), "Unexpected Error Occurred")

    def testLoadData1(self, testingNewDf=pd.DataFrame.from_dict(testingDict), tableName="testing"):
        """
        Negative Test Case for Load Data Method.
        Parameters:
            tableName (String) : Table to append to or create to.
            df (DataFrame) : Pandas DataFrame to get data from.
        Returns:
            Returns nothing if failed as expected.
        """
        self.conn.reconnect()
        self.assertIsNotNone(self.msc.loadData(testingNewDf, tableName),
                             "Table fields don't match. Cannot append data.")

    def testCreateObject(self, tableName="student", columnName="name"):
        """
        Positive test case for Create Object Method.
        Parameters:
            tableName (String) : Name of table to search for column in.
            columnName (String) : Name of column to duplicate and convert into new table.
        Returns:
            Returns nothing if successful.
        """
        self.conn.reconnect()
        self.assertIsNotNone(self.msc.createObject(self.cursor, tableName, columnName), "Unexpected Error Occurred")
        self.conn.close()

    def testCreateObject1(self, tableName="student", columnName="name"):
        """
        Negative test case for Create Object Method.
        Parameters:
            tableName (String) : Name of table to search for column in.
            columnName (String) : Name of column to duplicate and convert into new table.
        Returns:
            Returns nothing if failed as expected.
        """
        self.conn.reconnect()
        self.assertIsNone(self.msc.createObject(self.cursor, tableName, columnName), "Created Table's Name was Taken")
        self.conn.close()

    def testCreateObject2(self, tableName="student", columnName="error"):
        """
        Negative test case for Create Object Method.
        Parameters:
            tableName (String) : Name of table to search for column in.
            columnName (String) : Name of column to duplicate and convert into new table.
        Returns:
            Returns nothing if failed as expected.
        """
        self.conn.reconnect()
        self.assertIsNone(self.msc.createObject(self.cursor, tableName, columnName), "Invalid Column Name")
        self.conn.close()

    def testCreateObject3(self, tableName="error", columnName="name"):
        """
        Negative test case for Create Object Method.
        Parameters:
            tableName (String) : Name of table to search for column in.
            columnName (String) : Name of column to duplicate and convert into new table.
        Returns:
            Returns nothing if failed as expected.
        """
        self.conn.reconnect()
        self.assertIsNone(self.msc.createObject(self.cursor, tableName, columnName), "Invalid Table Name")
        self.conn.close()

    def testDropTable(self, tableName="name"):
        """
        Positive Test Case for Drop Table method.
        Parameters:
            tableName (String) : Name of table to delete.
            cursor (mySql Connection) : Cursor to execute mySql queries with.
        Returns:
            Returns nothing if successful.
        """
        self.conn.reconnect()
        self.assertIsNone(self.msc.dropTable(tableName, self.cursor), "Unexpected Error Occurred")
        self.conn.close()

    def testDropTable1(self, tableName="error"):
        """
        Negative Test Case for Drop Table method.
        Parameters:
            tableName (String) : Name of table to delete.
            cursor (mySql Connection) : Cursor to execute mySql queries with.
        Returns:
            Returns nothing if failed as expected.
        """
        self.conn.reconnect()
        self.assertIsNotNone(self.msc.dropTable(tableName, self.cursor), "Invalid table name")
        self.conn.close()

    def testReadQueryData(self, statement="Select * from student"):
        """
        Positive test case for ReadQueryData method.
        Parameters:
            statement (String) : Statement to execute and convert to dataframe.
        Returns:
            Returns nothing if successful.
        """
        self.conn.reconnect()
        self.assertIsNotNone((self.msc.readQueryData(statement, self.conn)), "Unexpected Error Occurred")
        self.conn.close()

    def testReadQueryData1(self, statement="Select * from error"):
        """
        Negative test case for ReadQueryData method.
        Parameters:
            statement (String) : Statement to execute and convert to dataframe.
        Returns:
            Returns nothing if failed as expected.
        """
        self.conn.reconnect()
        self.assertIsNone((self.msc.readQueryData(statement, self.conn)), "Invalid Statement")
        self.conn.close()


if __name__ == '__main__':
    unittest.main()
