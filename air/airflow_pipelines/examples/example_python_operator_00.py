#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example DAG demonstrating the usage of the TaskFlow API to execute Python functions natively and within a
virtual environment.
"""
from __future__ import annotations

import logging
import shutil
import sys
import tempfile
import time
from pprint import pprint

import pendulum

from airflow                  import DAG
from airflow.decorators       import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

BASE_DIR = tempfile.gettempdir()


def x():
    pass


with DAG(
    dag_id="example_python_operator_00",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:

    # [START howto_operator_python]
    @task(task_id="print_the_context")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return "Whatever you return gets printed in the logs"

    run_this = print_context()
    # [END howto_operator_python]

    # [START howto_operator_python_render_sql]
    @task(task_id="log_sql_query", templates_dict={"query": "sql/sample.sql"}, templates_exts=[".sql"])
    def log_sql(**kwargs):
        logging.info("Python task decorator query: %s", str(kwargs["templates_dict"]["query"]))

    log_the_sql = log_sql()
    # [END howto_operator_python_render_sql]

    # [START howto_operator_python_kwargs]
    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    for i in range(5):

        @task(task_id=f"sleep_for_{i}")
        def my_sleeping_function(random_base):
            """This is a function that will run within the DAG execution"""
            time.sleep(random_base)

        sleeping_task = my_sleeping_function(random_base=float(i) / 10)

        run_this >> log_the_sql >> sleeping_task
    # [END howto_operator_python_kwargs]
