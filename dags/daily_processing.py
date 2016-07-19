"""
Workflow definition for daily processing
"""

from __future__ import division, absolute_import, print_function

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import (
    BookData,
    Predict,
    Decide
)

dag_id = "daily_processing"
schedule_interval = '0 22 * * *'

default_args = {
    'owner': 'europython',
    'depends_on_past': False,
    'email': ['airflow@europython'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id,
    start_date=datetime(2016, 7, 15, 22, 0),
    schedule_interval=schedule_interval,
    default_args=default_args)

book = BookData(dag=dag)

predict = Predict(dag=dag)
predict.set_upstream(book)

decide = Decide(dag=dag)
decide.set_upstream(predict)
