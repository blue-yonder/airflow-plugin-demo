"""
Workflow definition to book data
"""

from __future__ import division, absolute_import, print_function

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import (
    BookData
)

dag_id = "book_data"
schedule_interval = None

default_args = {
    'owner': 'europython',
    'depends_on_past': False,
    'email': ['airflow@europython'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30)
}

dag = DAG(
    dag_id,
    start_date=datetime(2016, 12, 7),
    schedule_interval=schedule_interval,
    default_args=default_args)

book = BookData(dag=dag)
