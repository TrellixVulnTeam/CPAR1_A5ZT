import datetime
import pymysql.cursors
import pandas as pd
from secret.secret import secret
from sqlalchemy import create_engine

__user = secret().getUser()
__secret = secret().getSecret()

def toSQL(df,exist_method,table,db='Consensus_Reporting'):
    '''
    df: pandas dataframe to be uploaded to db
    exist_method: 'append' | 'replace'
    table: the name of the table, if table name is not in DB and
    replace is in exist_method will create a new table in db
    db:which database to connect to.
    '''
    engine = create_engine("mysql+pymysql://{}:{}@localhost:3309/{}".format(__user,__secret,db))
    df['cdate'] = datetime.datetime.today()
    df['cdate'] = df['cdate'].dt.date
    conn = engine.connect()

    try:
        df.to_sql(table,con=conn,if_exists=exist_method,index=False)
    except:
        print('Unsuccessful {}'.format(exist_method))
        raise
    else:
        print('Successful {}'.format(exist_method))
    finally:
        conn.close()
