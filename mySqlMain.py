#!/usr/bin/python

__author__ = "Nitin Nallagatla"
__email__ = "nitinrnallagatla@gmail.com"
__version__ = '0.0.3'

import argparse
import json
import logging
import sys
from argparse import RawTextHelpFormatter

import mysql.connector

import mySqlClass


def main():
    """
    Main method to run mySqlClass as per given requirements.
    """
    retcode = 0
    parser = argparse.ArgumentParser(
        description='Input method to  run and necessary parameters: Options:\n'
                    '--chosenMethod connectMethod --username "John" --password "14789" '
                    '--tableName "student" --usernameCol "name" --passwordCol "id"\n'
                    '--chosenMethod readTableData --tableName "student"\n'
                    '--chosenMethod readSampleData --input numRows "3" --tableName "student"\n'
                    '--chosenMethod readColumnData --tableName "student" --columnName "name\n'
                    '--chosenMethod findRow --searchDict {"name" : "John", "id" : 14789} --tableName "student"\n'
                    '--chosenMethod createObject --tableName "nameTable" --columnName "name"\n'
                    '--chosenMethod dropTable --tableName "student"\n'
                    '--chosenMethod readQueryData --statement "Select * from student"\n'
                    '--chosenMethod csvToSQL --path <your CSV file path> --tableName username\n'
                    '--chosenMethod sqlToCSV --path <your CSV file path> --tableName username\n',
        formatter_class=RawTextHelpFormatter)
    parser.add_argument('-username', "--username", help='input username')
    parser.add_argument('-password', "--password", type=int, help='input password')
    parser.add_argument('-usernameCol', "--usernameCol", help='input col of usernames')
    parser.add_argument('-passwordCol', "--passwordCol", help='input col of passwords')
    # parser.add_argument('-cursor', "--cursor", help='provide cursor object, created from mysql connection')
    parser.add_argument('-tableName', "--tableName", help='input tableName to operate on')
    # parser.add_argument('-conn', "--conn", help='input mysql connection to use')
    parser.add_argument('-numRows', "--numRows", help='input number of rows to use')
    parser.add_argument('-columnName', "--columnName", help='input column to operate on')
    parser.add_argument('-searchDict', "--searchDict", help='input dictionary as string to search for')
    parser.add_argument('-df', "--df", help='input data frame to fill table with')
    parser.add_argument('-statement', "--statement", help='input statement to run')
    parser.add_argument('-chosenMethod', "--chosenMethod", help='input function to run ')
    parser.add_argument('-path', '--path', help='path of file to be converted csv file to sql table to operate on ')
    # parser.add_argument('-d', '--searchDict', type=json.loads)

    args = vars(parser.parse_args())

    try:
        username = args["username"]
        password = args["password"]
        tableName = args["tableName"]
        usernameCol = args["usernameCol"]
        passwordCol = args["passwordCol"]
        statement = args["statement"]
        columnName = args["columnName"]
        numRows = args["numRows"]
        chosenMethod = args["chosenMethod"]
        searchDict = args["searchDict"]
        csvToSql = args["csvToSQL"]
        sqlToCSV = args["sqlToCSV"]
        path = args["path"]

        msc = mySqlClass.mySql()

        conn = mysql.connector.connect(user='test',
                                       password='password',
                                       database='sampleDatabase')

        cursor = conn.cursor()

        result = None
        # -searchDict "{'name' : 'John', 'id' : 14789}" --chosenMethod readColumnData --numRows 3 -columnName name -username John --password 14789 --tableName student --usernameCol n
        # ame --passwordCol id --statement "Select * from student"

        if chosenMethod == 'connectMethod':
            result = msc.connectMethod(username, password, tableName, usernameCol, passwordCol, cursor)
        elif chosenMethod == 'readTableData':
            result = msc.readTableData(tableName, conn)
        elif chosenMethod == 'readSampleData':
            result = msc.readSampleData(numRows, tableName, cursor)
        elif chosenMethod == 'readColumnData':
            result = msc.readColumnData(tableName, columnName, cursor)
        elif chosenMethod == 'findRow':
            result = msc.findRow(json.loads(searchDict), tableName, cursor)
        # elif input == 'loadData':
        #     result = msc.loadData(tableName, df)
        elif chosenMethod == 'createObject':
            result = msc.createObject(tableName, columnName, cursor)
        elif chosenMethod == 'dropTable':
            result = msc.dropTable(tableName, cursor)
        elif chosenMethod == 'readQueryData':
            result = msc.readQueryData(statement, conn)
        elif chosenMethod == 'csvToSQL':
            result = msc.csvToSQL(path, tableName)
        elif chosenMethod == 'sqlToCSV':
            result = msc.sqlToCSV(path, tableName)
        if result is not None:
            print(result)
        else:
            print("None")

        logging.info("HELLO")

        # my_dict = {
        #     'class': ['Five', 'Six', 'Three'],
        #     'No': [5, 2, 3]
        # }
        # df = pd.DataFrame(data = my_dict)

    except mysql.connector.Error as e:
        print(e)
        return -1
    except AttributeError as e:
        print(e)
        print(e)
        return -1
    except MemoryError as e:
        print(e)
        return -1
    except ReferenceError as e:
        print(e)
        return -1
    except SystemError as e:
        print(e)
        return -1
    except SystemExit as e:
        print(e)
        return -1
    except TypeError as e:
        print(e)
        return -1
    except KeyboardInterrupt as e:
        # log.exception("IO error: {0}".format(e))
        print(e)
        return -1
    except Exception as e:
        print(e)
        return -1
    finally:
        if conn is not None and conn.is_connected():
            conn.close()
    return retcode


if __name__ == "__main__":
    print("Starting application")
    retcode = main()
    sys.exit(retcode)
