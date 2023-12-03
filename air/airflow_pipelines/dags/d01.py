'''
Command line

# activate virtual environment
source /home/art/git/data_engineer/airflow_pipelines/venv/bin/activate

cd ~/airflow/

airflow dags list
# d01_animals

airflow tasks list d01_animals
# animals
# my_shell


# Test the tasks
airflow tasks run d01_animals animals 2023-01-01
airflow tasks test  d01_animals my_shell  2023-01-01


'''

from datetime import datetime, timedelta

import pandas as pd
import pendulum
import sys
import tempfile

import airflow
from airflow                    import DAG
from airflow.decorators         import task
from airflow.operators.python   import PythonOperator
from airflow.operators.bash     import BashOperator
from airflow.operators.empty    import EmptyOperator

PATH_TO_PYTHON_BINARY = sys.executable
BASE_DIR = tempfile.gettempdir()


args = {
    'owner'      : 'airflow',
    'start_date' : airflow.utils.dates.days_ago( 1 ) ,
}

with DAG(
    dag_id      = 'd01_animals',

    schedule    = None,
    #schedule_interval="@daily",

    default_args= args,
    start_date  = pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup     = False,
    tags        = [ 'animals', 'wild' ]
) as dag:

    @task( task_id = 'animals' )
    def fun_animals(*args, **kwargs):
        print('\n\n my_fun(), we like animals! \n\n ')
        print('\n kwargs: {} \n'.format(kwargs))
        return 'my_fun() finished OK'

    animals = fun_animals( turttles ='ninjas', oboe ='squirword', michael_angelo = 'nunchakus'  )

    my_shell = BashOperator(
        task_id = 'my_shell',
        bash_command = 'echo "\n\n  obstacle -> pearl \n\n" '
    )

animals >> my_shell




