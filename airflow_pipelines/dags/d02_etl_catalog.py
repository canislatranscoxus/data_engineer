'''
This DAG is an ETL pipeline that move data from csv file to mysql.

We need to create commections in airflow GUI:

    * data_path     = directory where is the csv data file
    * mysql_conn_id = The connection to mysql database


-----------------------------------------------------

Command line

source /home/art/git/data_engineer/airflow_pipelines/venv/bin/activate

cd ~/airflow/
-----------------------------------------------------

# run scheduler
airflow scheduler

# run webserver
airflow webserver -p 8080

-----------------------------------------------------
Test our DAG

airflow dags list
# d02_etl_catalog

airflow tasks list d02_etl_catalog
# extract
# transform
# load


# Test the tasks
airflow tasks run d02_etl_catalog transform 2023-01-01
airflow tasks test  d02_etl_catalog my_shell  2023-01-01


'''

from datetime import datetime, timedelta

import json
import os
import pandas as pd
import pendulum
import sys
import tempfile

from sqlalchemy import create_engine

import airflow
from airflow                    import DAG
from airflow.decorators         import task
from airflow.hooks.base         import BaseHook
from airflow.operators.python   import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

from airflow.operators.bash     import BashOperator
from airflow.operators.empty    import EmptyOperator

PATH_TO_PYTHON_BINARY = sys.executable
BASE_DIR = tempfile.gettempdir()

data_path = f'{json.loads(BaseHook.get_connection("data_path").get_extra()).get("path")}data.csv'
transformed_path = f'{os.path.splitext(data_path)[0]}-transformed.csv'


default_args = {
    'owner'         : 'airflow',
    'start_date'    : airflow.utils.dates.days_ago( 1 ) ,
    'mysql_conn_id' : 'mysql_conn_id'
}

with DAG(
    dag_id      = 'd02_etl_catalog',

    schedule    = None,
    #schedule_interval="@daily",

    default_args= default_args,
    start_date  = pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup     = False,
    tags        = [ 'ETL', 'csv', 'mySql' ]
) as dag:

    @task( task_id = 'transform' )
    def fun_transform(*args, **kwargs):
        print('\n\n fun_extract(), extract data from csv ! \n\n ')
        print('kwargs: {}'.format(kwargs))

        print( '\n\n ' )
        print( 'data_path       : {}'.format(  data_path        ))
        print('transformed_path : {}'.format( transformed_path  ))

        df = pd.read_csv( filepath_or_buffer=data_path,
                          sep=',',
                          header=0,
                          converters = {'sku': str, 'pic': str} )

        df.sku.replace( to_replace=dict( NULL=None ), inplace=True)

        df.to_csv(path_or_buf=transformed_path)
        print( df )
        print('\n\n ')

        return 'fun_transform() finished OK'

    transform = fun_transform( turttles ='ninjas', oboe ='squidword'  )


    test_mysql_task = MySqlOperator(
        task_id = "test_mysql_task",
        sql     = 'select * from lz_catalog_05;',
    )

    clean_table = MySqlOperator(
        task_id = "clean_table",
        sql     = 'delete from lz_catalog_05;',
    )

    @task( task_id = 'load' )
    def fun_load(*args, **kwargs):
        df = pd.read_csv( filepath_or_buffer=data_path,
                          sep=',',
                          header=0,
                          converters = {'sku': str, 'pic': str} )

        #engine = create_engine('postgresql://airflow:airflow@postgres/pluralsight')

        conn = BaseHook.get_connection( 'mysql_conn_id' )
        print( '\n\n fun_load(), \n ' )
        print('type( conn) : {}'.format(type(conn)))
        print( 'conn:' )
        print( conn )

        print( 'conn attributes: ' )
        print( vars( conn ) )

        print( '\n\n' )


        conn_str = 'mysql+pymysql://{}:{}@{}/{}'.format(
            conn.login,
            conn._password,
            conn.host,
            conn.schema
        )
        print( 'conn_str: {}'.format( conn_str ) )

        engine = create_engine( conn_str, pool_recycle=3600)
        #dbConnection = sqlEngine.connect()

        df.to_sql( "lz_catalog_05",
                    engine,
                    if_exists='append',
                    chunksize=500,
                    index=False
                    )

        return '\n\n fun_load() finished OK \n\n'


    load = fun_load()

    my_shell = BashOperator(
        task_id = 'my_shell',
        bash_command = 'echo "\n\n  obstacle -> pearl \n\n" '
    )

transform >> test_mysql_task >> clean_table >> load >> my_shell




