import time
import random

from airflow import models
from airflow import utils as airflow_utils


class Predict(models.BaseOperator):
    @airflow_utils.apply_defaults
    def __init__(self, country=None, **kwargs):
        task_id = 'predict'
        if country:
            task_id = task_id + '_' + country
        super(Predict, self).__init__(
            task_id=task_id,
            **kwargs)

    def execute(self, context):
        waiting_time = 3 + random.random() * 3
        time.sleep(waiting_time)
