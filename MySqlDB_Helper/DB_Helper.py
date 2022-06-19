from django import db
import mysql.connector as connector
from mysql.connector import DatabaseError

class DB_Helper:

    def __int__(self, host_name, port_no, user_name, password, database_name):
        self.host_name = host_name
        self.port_no = port_no
        self.user_name = user_name
        self.password = password
        self.database_name = database_name
        self.connection = None

    def __init__(self):
        self.user_name = None
        self.port_no = None
        self.host_name = None
        self.password = None
        self.database_name = None
        self.connection = None

    def get_host_name(self):
        return self.host_name

    def get_user_name(self):
        return self.user_name

    def get_password(self):
        return self.password

    def get_database_name(self):
        return self.database_name

    def get_portNo(self):
        return self.port_no

    def get_connection(self):
        return self.connection

    def set_host_name(self, host):
        self.host_name = host

    def set_port_no(self, port):
        self.port_no = port

    def set_user_name(self, user):
        self.user_name = user

    def set_password(self, password):
        self.password = password

    def set_database_name(self, db_name):
        self.database_name = db_name

    def set_connection(self, conn):
        self.connection = conn

    def create_DB_Connection(self, host=str(""), port=str(""), user_name=str(""), password=str(""), db_name=str("")):
        """

            -> it s used for connection & performing operation with sql database.

            Parameter :

                1) host :
                -> provide host name of the database.
                -> Default value None.

                2) port :
                -> provide port no. of running no database.
                -> Default value None.

                3) user
                -> user = provide username of database connection.
                -> Default value None.

                4) password
                -> providing password for accessing Database.
                -> Default value None.

                5) database
                -> providing database name for communication with database.
                -> Default value None.

            Ex. -
                create_DB_Connection("localhost", "3306", "root", "", "pythontest")

        return : type is dict()
                key = message, status, data

                data has return connection obj.

        """

        data = dict()
        data['message'] = None
        data['status'] = None
        data['data'] = False
        try:
            if (((host != "") and (port != "") and (user_name != "") and (db_name != "")) or (password != "")):

                self.connection = connector.connect(host=host, port=port, user=user_name, password=password,
                                                    database=db_name)
                data['message'] = "ok"
                data['status'] = 200
                data['data'] = self.connection
            else:
                raise Exception(
                    f"EmptyValueException : any one or all parameter values are None...  \nParameter Values : \nhost = {host}, port={port}, user_name={user_name}, password={password}, db_name={db_name} ")
        except DatabaseError as db_execep:
            data['message'] = db_execep
            data['status'] = 201
        except Exception as e:
            data['message'] = e
            data['status'] = 201
        finally:
            return data

    def create_Database(self, db_name=str("")):
        """

        -> it is used for create database which is not already exists.

        Parameter :
            1) db_name
            -> providing database name to create it and provide value in form of string.
            -> Default value None.

        Ex. - create_Database("login")

        return : type is dict()
                key = message, status, data

                data has return  Boolean.

        """
        data = dict()
        data['message'] = None
        data['status'] = None
        data['data'] = False
        try:

            # check value is not empty
            if db_name != "":

                cursor = self.connection.cursor()
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                data['data'] = True
                data['message'] = "ok"
                data['status'] = 200

            else:

                raise Exception(
                    f"EmptyValueError : any one or all parameter values are None...  \nParameter Values : \n Database Name = {db_name}")

        except DatabaseError as db_execep:

            data['message'] = db_execep
            data['status'] = 201

        except Exception as execp:

            data['message'] = execp
            data['status'] = 201
        finally:
            return data

    def create_Table(self, table_name="", table_column=list()):
        """

        -> it is used for create table which is not already exists.

        Parameter :

                1) table_name :
                -> Provide table name to create it into selected database.
                -> Provide value in string.
                -> Default value is None.

                2) columns :
                -> Provide column name with datatype & others.
                -> Provide values in list.
                -> Default value is None.

        return : type is dict().
                 key are message,status,data.
                 data has boolean value.

        Ex. -
            create_table("user_details", ["id int(5) PRIMARY KEY AUTO_INCREMENT", "name varchar(20)"])
        """
        data = dict()
        data['message'] = None
        data['status'] = None
        data['data'] = False
        try:
            if (table_name != "") and (table_column != []):
                cursor = self.connection.cursor()
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ( {', '.join(table_column)})")
                data['message'] = "ok"
                data['status'] = 201
                data['data'] = True
            else:
                raise Exception(f"EmptyValueError : table name or column name parameter values are None...  \nParameter Values : \n table_name = {table_name}, table_column = {table_column}")
        except DatabaseError as db_execep:
            data['message'] = db_execep
            data['status'] = 201
        except Exception as execp:
            data['message'] = execp
            data['status'] = 201
        finally:
            self.connection.commit()
            return data

    def insert_records(self, table_name="", column_name=list(), column_values=list(), multiple_record=False):
        """

        -> it is use for inserting single/multiple rows

        Parameter :

            1) table_name : provide table name to update record.
                            Default value is ""(Empty String).

            2) column_name : provide column name want to insert records and value type is list().
                             Default value is list().

            3) column_value : provide column values want to insert records and value type is list().
                              Default value is list().

            4) multiple_record : you want to add multiple records at time use and value type is Boolean.
                                 Default value is False.
                                 multiple_record = True : it means you want to insert multiple records in simmilar table.

        return : type is dict().
                 key are message,status,data.
                 data value is no. affected rows.

        Ex. -
            -> insert single record
            insert_records("user",["name","phone"], ("Sagar","9033930035"))

            -> insert multiple records
            insert_records("user",["name","phone"],(("Divyesh","9033930035"), ("Sagar","9033930035")), multiple_record=True)


        """

        data = dict()
        data['message'] = None
        data['status'] = None
        data['data'] = False
        query = ""
        try:

            cursor = self.connection.cursor()

            if table_name != "":

                if column_name != [] and column_values != []:

                    # for insert only single record
                    if multiple_record != True:
                        query = f"INSERT INTO {table_name}({','.join(column_name)}) VALUES {column_values}"

                        print(query)

                        cursor.execute(query)

                    else:

                        query = f"INSERT INTO {table_name}({','.join(column_name)}) VALUES ({','.join(['%s'] * len(column_name))})"
                        cursor.executemany(query, column_values)

                    data["data"] = cursor.rowcount
                    data["message"] = "ok"
                    data["status"] = 200

            else:

                raise Exception(f"EmptyValueError : table_name parameter value is None...  \nParameter Values : \n table_name = {table_name}")

        except DatabaseError as db_execep:

            data['message'] = db_execep
            data['status'] = 201

        except Exception as execp:

            data['message'] = execp
            data['status'] = 201

        finally:

            self.connection.commit()
            self.connection.close()
            return data

    def fetch_records(self, table_name="", column_name=list("*"), where="", where_clause=False):
        """
        -> it is use for fetch records using where clause or without where clause.

        Parameter :

            1) table_name : provide table name to update record.
                            Default value is ""(Empty String).

            2) column_name : provide column name which column values want to select and type is list.
                             Default values is list("*").

            3) where : provide where condition.
            
            4) where_clause : it  is Boolean Value. for fetch records using where clause.
                              Default values is False.
                              if it is true then fetch records using where clause.else all records means withour where clause.

        return : type is dict()
                 key has message,status,data
                 data has fetch records.

        Ex. -
            -> fetch record without where clause.
            fetch_records("user",["name","phone"])

            -> fetch records with where clause.
            fetch_records("fb_blogger_details", ["BLGRD_Gender", "BLGRD_Email"], "BLGRD_ID > 2 and BLGRD_Email='abc@gmail.com'", where_clause=True)
        """
        data = dict()
        data['message'] = None
        data['status'] = None
        data['data'] = False
        query = ""
        cursor = self.connection.cursor()
        try:

            if table_name != "":

                if column_name != []:

                    if where_clause != True:

                        query = f"SELECT {(', '.join(column_name))} FROM {table_name}"

                    elif where_clause == True:
                        query = f"SELECT {', '.join(column_name)} FROM {table_name} WHERE {where}"
                        print(query)

                    cursor.execute(query)
                    data["message"] = "ok"
                    data['status'] = 200
                    data['data'] = cursor.fetchall()
                else:

                    raise Exception(f"EmptyValueError : table_name parameter value is None...  \nParameter Values : \n table_name = {table_name}")

            else:

                raise Exception(f"EmptyValueError : table_name parameter value is None...  \nParameter Values : \n table_name = {table_name}")

        except DatabaseError as db_execep:

            data['message'] = db_execep
            data['status'] = 201

        except Exception as execp:

            data['message'] = execp
            data['status'] = 201

        finally:
            return data

    def update_record(self, table_name="", column_name=list(), column_value=list(), where_condition=""):
        """

        -> it is used to updated single or multiple records based on where condition.

        Parameters :
            1) table_name : provide table name to update record.
                            Default value is ""(Empty String).

            2) column_name : provide column name which column values want to change and type is list.
                             Default values is Empty list.

            3) column_value : provide column values which want to change values and type is list.
                              Default values is Empty list.

            4) where_condition : provide condition.
                                 that condition based update records values.

        return : type is dict()
                 key is message,status,data

                 data has no. of affected rows.

        Ex. -
            
            update_record("fb_blogger_details", ["BLGRD_Gender", "BLGRD_Email"], ["F", "abc@gmail.com"],"BLGRD_ID=1")
        
        """
        data = dict()
        data['message'] = None
        data['status'] = None
        data['data'] = False
        query = ""
        cursor = self.connection.cursor()
        try:

            if table_name != "":

                # check list is not empty
                if column_name != [] and column_value != [] and where_condition != "":

                    column_key_val = [f"{column_name[i]}='{column_value[i]}'" for i in range(len(column_name))]
                    
                    query = f"UPDATE {table_name} SET {','.join(column_key_val)} WHERE {where_condition}"

                    print(query)

                    cursor.execute(query)

                    data["message"] = "ok"
                    data['status'] = 200
                    data['data'] = cursor.rowcount

                # check any one list is empty
                elif (not column_name) or (not column_value) or (not where_condition):

                    raise Exception(f"EmptyListException : either any one or all parameter values are empty list or not list   \nParameter Values : \n  column_name={column_name}, column_value={column_value}, where_condition = {where_condition}")

                # both condition are false go to else
                else:
                    raise Exception(f"EmptyListException : either any one or all parameter values are empty list or not list   \nParameter Values : \n  column_name={column_name}, column_value={column_value}, where_key={where_key}, where_value={where_value}")

            else:
                raise Exception(f"EmptyValueError : table_name parameter value is None...  \nParameter Values : \n table_name = {table_name}")

        except DatabaseError as db_execep:
            data['message'] = db_execep
            data['status'] = 201

        except Exception as execp:
            data['message'] = execp
            data['status'] = 201

        finally:
            self.connection.commit()
            return data

    def delete_record(self, table_name="", where_condition=""):
        """

        -> it is used for deleting record.

        Parameters :
            1) table_name = provide table name for insert records.
            2) where_condition = provide condition based on our condition and value delete records.
        Ex. -
            -> for deleting Record
            delete_record("fb_blogger_details", "BLGRD_ID=4")

        return : type is dict().
                key are message,status and data.
                data value is affected row.

        """

        data = dict()
        data['message'] = None
        data['status'] = None
        data['data'] = False
        query = ""
        cursor = self.connection.cursor()
        try:

            # table name is not empty
            if table_name != "":

                # for where_key and where_value is not empty list
                if where_condition != "" :

                    query = f"DELETE FROM {table_name} WHERE {where_condition}"
                    print(query)
                    cursor.execute(query)

                    data['data'] = cursor.rowcount
                    data['message'] = "ok"
                    data['status'] = 200

                else:

                    raise Exception(f"EmptyValueError : where_key and where_value parameter value are None...  \nParameter Values : \n where_condition : {where_condition}")

            else:

                raise Exception(f"EmptyValueError : table_name parameter value is None...  \nParameter Values : \n table_name = {table_name}")

        except DatabaseError as db_execep:

            data['message'] = db_execep
            data['status'] = 201

        except Exception as execp:

            data['message'] = execp
            data['status'] = 201

        finally:

            self.connection.commit()
            # self.connection.close()
            return data

    def my_custome_select_query(self, query=""):

        data = dict()
        data['message'] = None
        data['status'] = None
        data['data'] = False

        try:

            if query != "":
                
                cursor = self.connection.cursor()
                cursor.execute(query)

                data["message"] = "ok"
                data['status'] = 200
                data['data'] = cursor.fetchall()

            else:

                raise TypeError("Query is Empty")
        
        except TypeError as typeErr:

            data['message'] = typeErr
            data['status'] = 201

        except Exception as e:

            data['message'] = e
            data['status'] = 201
        
        finally:
            return data

    def close_connection(self):
        self.connection.close()
