import configparser
import MySQLdb
from sqlalchemy import create_engine
import pandas as pd
from CHECK.secret import secret

class DatabaseConnect():
    def __init__(self,database):
        _sec = secret.secret()
        self.hostname = _sec.getHost()
        self.username = _sec.getUser()
        self.password = _sec.getSecret()
        self.port = _sec.getPort()
        self.database = database
        self.engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(self.username,
                                                                            self.password,
                                                                            self.hostname,
                                                                            self.port,
                                                                            self.database))

    def connection_obj(self):
        connection = self.engine.connect()
        return connection

    def query(self,sql_str,df_flag=True,parse_dates=None,chunksize=None,columns=None):
        '''
        sql_str(str): query to be fetched from data base
        returns a pandas dataframe that contains query results
        '''
        try:
            connection = self.connection_obj()
            if df_flag == True:
                df = self.to_dataframe(sql_str,connection,parse_dates,chunksize=None)
                return df
            else:
                connection.execute(sql_str)
            connection.close()
        except:
            raise Exception


    def to_dataframe(self,sql_str, connection, parse_dates=None, chunksize=None):
        df = pd.read_sql(sql_str,con=connection,parse_dates=parse_dates,chunksize=None)
        return df

    def insert(self,df,tbl,chunksize=None):
        '''
        df(pd.DataFrame): df to be inserted
        tbl(str): table in database to insert df into
        '''
        try:
            connection = self.connection_obj()
            df.to_sql(name=tbl,con=connection,if_exists='append',index=False,chunksize=None)
            connection.close()
        except:
            raise Exception

    def stored_procedure(self, proc_name, proc_params=None):
        '''Used to call a stored procedure;
        proc_params(list): list of parameters'''

        connection = self.engine.raw_connection()
        try:
            cursor = connection.cursor()
            if proc_params == None:
                cursor.callproc(proc_name)
            else:
                cursor.callproc(proc_name, proc_params)
            results = list(cursor.fetchall())
            cursor.close()
            connection.commit()
        finally:
            connection.close()


    def replace(self,df,tbl,chunksize=None):
        '''
        df(pd.DataFrame): df to be inserted
        tbl(str): table in database to insert df into
        '''
        try:
            connection = self.connection_obj()
            self.query("DELETE FROM {}".format(tbl), df_flag=False)
            df.to_sql(name=tbl,con=connection,if_exists='append',index=False,chunksize=None)
            connection.close()
        except:
            raise Exception

    def inline_import(self, sql_str, file_path):
        '''sql_str: (string) contains the inline file instructions
           file_path: (string) path to file to verify counts'''
        connection = MySQLdb.Connect(host=self.hostname, user=self.username, passwd=self.password, db=self.database)
        cursor = connection.cursor()
        n_rows = cursor.execute(sql_str)
        cursor.close()
        connection.commit()
        connection.close()
        if file_path != None:
            file_rows = sum([1 for i in open(file_path)])
            return n_rows, file_rows
        return n_rows
