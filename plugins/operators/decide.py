import time
import logging

from airflow import models
from airflow import utils as airflow_utils
from airflow import hooks


class Decide(models.BaseOperator):
    @airflow_utils.apply_defaults
    def __init__(self, **kwargs):
        super(Decide, self).__init__(
            task_id='decide',
            **kwargs)
        self.http_conn_id = 'DECISION_SERVER'
        self.endpoint_job_start = 'decide/'
        self.endpoint_job_status = 'job_status/'

    def execute(self, context):
        http = hooks.HttpHook(method='POST', http_conn_id=self.http_conn_id)
        response = http.run(endpoint=self.endpoint_job_start)
        job_id = response.json()['job_id']
        logging.info('started decision job with job id {}'.format(job_id))
        self.wait_for_job(job_id)

    def wait_for_job(self, job_id):
        job_status = None
        http = hooks.HttpHook(method='GET', http_conn_id=self.http_conn_id)
        while not job_status == 'FINISHED':
            time.sleep(1)
            response = http.run(endpoint=self.endpoint_job_status + str(job_id))
            job_status = response.json()['status']
            logging.info('status of decision job {} is {}'.format(job_id, job_status))
