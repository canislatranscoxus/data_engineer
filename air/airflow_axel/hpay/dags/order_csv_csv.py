'''
Description: This DAG represent a pipeline to clean and move CSV files
            from one directory to another in the Data Lake.

                ┌───────────┐       ┌───────────┐         ┌───────────┐
                │    CSV    │ --->  │  Airflow  │ --->    │    CSV    │
                └───────────┘       └───────────┘         └───────────┘

Usage:
run locally:

# start Airflow

# copy python file to Dag's directory
/home/art/airflow/airflow standalone

'''
import json
import os
import pandas as pd
import pendulum
import sys
import tempfile

#from sqlalchemy import create_engine

import airflow
from airflow            import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
#from airflow.operators.python import  PythonOperator
#from airflow.operators.postgres_operator import PostgresOperator

PATH_TO_PYTHON_BINARY = sys.executable
BASE_DIR = tempfile.gettempdir()

j       = json.loads(BaseHook.get_connection("hpay_path").get_extra() )
in_path  = os.path.join( j[ 'in'  ],  'order.csv' )
out_path = os.path.join( j[ 'out' ],  'order.csv' )

default_args = {
    'owner'         : 'airflow',
    'start_date'    : airflow.utils.dates.days_ago( 1 ) ,
}

with DAG(
        dag_id='hpay_order_csv_csv',
        schedule=None,
        # schedule_interval="@daily",
        default_args=default_args,
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
        tags=['ETL', 'csv', 'data_lake' ]
) as dag:

    # This file could come in S3 from our ecommerce application
    is_new_data_available = FileSensor(
        task_id       = "is_new_data_available",
        fs_conn_id    = "data_path",
        filepath      = "data.csv",
        poke_interval = 5,
        timeout       = 20
    )

    @task( task_id = 'transform' )
    def fun_transform(*args, **kwargs):
        print('\n\n fun_extract(), extract data from csv ! \n\n ')
        print('kwargs: {}'.format(kwargs))
        print( '\n\n ' )
        print( 'in_path : {}'.format(  in_path  ))
        print('out_path : {}'.format( out_path  ))

        df = pd.read_csv( filepath_or_buffer= in_path,
                          sep        =',',
                          header     =1,
                          #converters = {'sku': str, 'pic': str}
                          )

        #df.sku.replace( to_replace=dict( NULL=None ), inplace=True)
        df.to_csv(path_or_buf= out_path)
        print( df )
        print('\n\n ')
        return 'fun_transform() finished OK'

    transform = fun_transform( turttles ='ninjas', oboe ='squidword'  )


    my_shell = BashOperator(
        task_id = 'my_shell',
        bash_command = 'echo "\n\n  MK: Finish it ! \n\n" '
    )









