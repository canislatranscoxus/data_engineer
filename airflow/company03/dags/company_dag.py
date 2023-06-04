import json
import os
from datetime import datetime, timedelta

from airflow.contrib.operators.slack_webhook_operator import \
    SlackWebhookOperator
from sqlalchemy import create_engine

import pandas as pd
from airflow                                import DAG
from airflow.contrib.sensors.file_sensor    import FileSensor
from airflow.hooks.base_hook                import BaseHook
from airflow.operators.postgres_operator    import PostgresOperator
from airflow.operators.python_operator      import PythonOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 11, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "axel.sirota@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

#data_path = f'{json.loads(BaseHook.get_connection("data_path").get_extra()).get("path")}/data.csv'
#transformed_path = f'{os.path.splitext(data_path)[0]}-transformed.csv'

data_dir = f'{json.loads(BaseHook.get_connection("data_path").get_extra()).get("path")}/'

csv_files = {
    'jobs' : {
        'data_path'        : os.path.join( data_dir, 'jobs.csv'       ),
        'transformed_path' : os.path.join( data_dir, 'trans-jobs.csv' ),
        'table_name'       : 'jobs',
        'usecols'          : ['id', 'job'],
    },

    'departments' : {
        'data_path'        : os.path.join( data_dir, 'departments.csv'       ),
        'transformed_path' : os.path.join( data_dir, 'trans-departments.csv' ),
        'table_name'       : 'departments',
        'usecols'          : ['id', 'department'],
    },

    'hired_employees' : {
        'data_path'        : os.path.join( data_dir, 'hired_employees.csv'       ),
        'transformed_path' : os.path.join( data_dir, 'trans-hired_employees.csv' ),
        'table_name'       : 'hired_employees',
        'usecols'          : [ 'id', 'name', 'datetime', 'department_id', 'job_id' ],
    }
}





slack_token = BaseHook.get_connection("slack_conn").password


'''def transform_data(*args, **kwargs):
    invoices_data = pd.read_csv(filepath_or_buffer=data_path,
                                sep=',',
                                header=0,
                                usecols=['StockCode', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country'],
                                parse_dates=['InvoiceDate'],
                                index_col=0
                                )
    invoices_data.to_csv(path_or_buf=transformed_path)
'''



def transform_data( params ):
    # params : dictionary with metadata of a csv file

    row_data = pd.read_csv(filepath_or_buffer = params[ 'data_path' ],
                                sep           = ',',
                                header        = 0,
                                usecols       = params['usecols'],
                                index_col     = 0
                                )
    row_data.to_csv(path_or_buf = params['transformed_path'] )



def load_in_db( params ):
    transformed_data = pd.read_csv( params['transformed_path'] )
    transformed_data.columns = [c.lower() for c in
                                    transformed_data.columns]  # postgres doesn't like capitals or spaces

    transformed_data.dropna(axis=0, how='any', inplace=True)
    engine = create_engine(
        'postgresql://airflow:airflow@postgres/company01')

    transformed_data.to_sql(    name        = params['table_name'],
                                con         = engine,
                                if_exists   = 'append',
                                chunksize   = 500,
                                index       = False
                                )



with DAG(dag_id="company_dag",
         schedule_interval="@daily",
         default_args=default_args,
         template_searchpath=[f"{os.environ['AIRFLOW_HOME']}"],
         catchup=False) as dag:

    # This file could come in S3 from our ecommerce application
    is_new_jobs_csv = FileSensor(
        task_id         = "is_new_jobs_csv",
        fs_conn_id      = "data_path",
        filepath        = "jobs.csv",
        poke_interval   = 5,
        timeout         = 20
    )
    is_new_departments_csv = FileSensor(
        task_id         = "is_new_departments_csv",
        fs_conn_id      = "data_path",
        filepath        = "departments.csv",
        poke_interval   = 5,
        timeout         = 20
    )


    transform_jobs = PythonOperator(
        task_id         = "transform_jobs",
        python_callable = transform_data,
        op_kwargs       = { 'params': csv_files[ 'jobs' ] },
    )

    transform_departments = PythonOperator(
        task_id         = "transform_departments",
        python_callable = transform_data,
        op_kwargs       = { 'params': csv_files[ 'departments' ] },
    )


    create_jobs = PostgresOperator(
        task_id         = "create_jobs",
        sql             = ['create_jobs.sql'],
        postgres_conn_id= 'postgres',
        database        = 'company01',
        autocommit      = True
    )

    create_departments = PostgresOperator(
        task_id         = "create_departments",
        sql             = ['create_departments.sql'],
        postgres_conn_id= 'postgres',
        database        = 'company01',
        autocommit      = True
    )


    load_jobs = PythonOperator(
        task_id         = 'load_jobs',
        python_callable = load_in_db,
        op_kwargs       = { 'params': csv_files[ 'jobs' ] },
    )
    load_departments = PythonOperator(
        task_id         = 'load_departments',
        python_callable = load_in_db,
        op_kwargs       = { 'params': csv_files[ 'departments' ] },
    )
    



    '''create_report = PostgresOperator(
        task_id="create_report",
        sql=['exec_report.sql'],
        postgres_conn_id='postgres',
        database='company01',
        autocommit=True
    )'''

    notify_data_science_team = SlackWebhookOperator(
        task_id='notify_data_science_team',
        http_conn_id='slack_conn',
        webhook_token=slack_token,
        message="Data Science Notification \n"
                "New Invoice Data is loaded into invoices table. \n "
                "Here is a celebration kitty: "
                "https://www.youtube.com/watch?v=J---aiyznGQ",
        username='airflow',
        icon_url='https://raw.githubusercontent.com/apache/'
                 'airflow/master/airflow/www/static/pin_100.png',
        dag=dag
    )

    # Now could come an upload to S3 of the model or a deploy step

    '''
    is_new_data_available >> transform_data
    transform_data >> create_table >> save_into_db
    save_into_db >> create_departments >> create_jobs
    create_jobs >> create_hired_employees
    save_into_db >> notify_data_science_team
    save_into_db >> create_report
    '''

    #create_table >> create_jobs >> create_departments >> create_hired_employees

    is_new_jobs_csv >> transform_jobs >> create_jobs
    create_jobs >> load_jobs

    is_new_departments_csv >> transform_departments >> create_departments
    create_departments >> load_departments




