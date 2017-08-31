import datetime
import pymysql.cursors
import pandas as pd
from secret.secret import secret
from sqlalchemy import (MetaData, Table, Column, Integer, Numeric, String,
                        DateTime, create_engine, delete)

__user = secret().getUser()
__secret = secret().getSecret()

def toSQL(df,exist_method,table,db='Consensus_Reporting'):
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
