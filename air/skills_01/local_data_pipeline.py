'''
Crucial Local Setup Steps to Make This WorkTo run this without errors, 
you need to configure your local Airflow connection for Spark:

1. Open your Airflow Web UI (usually localhost:8080).

2. Navigate to Admin -> Connections.

3. Click the + (plus) icon to add a new connection.

4. Fill out the fields exactly like this:

    Connection Id   : spark_local
    Connection Type : Spark
    Host            : local[*] (This tells Spark to use all available cores on your laptop)

'''

from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='local_spark_and_beam_orchestration',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger for learning/testing
    catchup=False,
    tags=['learning', 'local'],
) as dag:

    # 1. ORCHESTRATE LOCAL SPARK
    # Make sure you create a connection in Airflow UI with Conn ID: 'spark_local'
    # Conn Type: Spark, Host: local[*]
    run_spark_job = SparkSubmitOperator(
        task_id='run_local_spark_job',
        application='/absolute/path/to/your/local_spark_script.py',
        conn_id='spark_local',
        total_executor_cores='2',
        executor_memory='1g',
        driver_memory='1g',
    )

    # 2. ORCHESTRATE LOCAL BEAM
    run_beam_job = BeamRunPythonPipelineOperator(
        task_id='run_local_beam_job',
        py_file='/absolute/path/to/your/local_beam_script.py',
        runner='DirectRunner',  # Forces Beam to execute on your local laptop CPU
        pipeline_options={
            'output': '/absolute/path/to/your/local_output_directory/'
        },
        py_interpreter='python3',
    )

    # Define the dependency (Run Spark first, then Beam)
    run_spark_job >> run_beam_job
