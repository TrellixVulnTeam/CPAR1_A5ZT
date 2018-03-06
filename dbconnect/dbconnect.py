import configparser
from sqlalchemy import create_engine
import pandas as pd
from secret import secret

class DatabaseConnect():
    def __init__(self,database):
        _sec = secret.secret()
        self.hostname = _sec.getHost()
        self.username = _sec.getUser()
        self.password = _sec.getSecret()
        self.port = _sec.getPort()
        self.database = database

    def connection_obj(self):
        engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(self.username,
                                                                       self.password,
                                                                       self.hostname,
                                                                       self.port,
                                                                       self.database))
        connection = engine.connect()
        return connection

    def query(self,sql_str,df_flag=True,parse_dates=None,chunksize=None):
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
