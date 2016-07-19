import time
import random

from airflow import models
from airflow import utils as airflow_utils


class BookData(models.BaseOperator):
    @airflow_utils.apply_defaults
    def __init__(self, **kwargs):
        super(BookData, self).__init__(
            task_id='book_data',
            **kwargs)

    def execute(self, context):
        waiting_time = 2 + random.random() * 2
        time.sleep(waiting_time)
