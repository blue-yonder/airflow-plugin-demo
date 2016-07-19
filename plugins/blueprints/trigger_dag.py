import httplib
from datetime import datetime
from flask import Blueprint, request, jsonify, Response
from sqlalchemy import and_
from sqlalchemy.orm.exc import NoResultFound

from airflow import settings
from airflow.models import DagRun, DagModel
from airflow.www.app import csrf
from airflow.utils.state import State


TriggerBlueprint = Blueprint('trigger', __name__, url_prefix='/trigger')
"""
Represents a blueprint to trigger DAGs.
"""


def check_dag_exists(session, dag_id):
    """
    if returns an error response, if it doesn't exist
    """
    dag_exists = session.query(DagModel).filter(DagModel.dag_id == dag_id).count()
    if not dag_exists:
        return Response('Dag {} does not exist'.format(dag_id), httplib.BAD_REQUEST)

    return None


@TriggerBlueprint.route('/<dag_id>', methods=['POST'])
@csrf.exempt
def trigger_dag(dag_id):
    """
    .. http:post:: /trigger/<dag_id>/

        Triggers a defined DAG. The data  must be send in json format with
        a key "run_id" and the value a string of your choice. Passing the data
        is optional. If no data is passed the run_id will be automatically
        be generated with a timestamp and looks like
        "external_trigger_2016-01-19T02:01:49.703365".

        **Example request**:

        .. sourcecode:: http

            POST /trigger/make_fit
            Host: localhost:7357
            Content-Type: application/json

            {
              "run_id": "my_special_run"
            }

        **Example response**:

        .. sourcecode:: http

            HTTP/1.1 200 OK
            Vary: Accept
            Content-Type: application/json

            {
              "dag_id": "daily_processing",
              "run_id": "my_special_run"
            }
    """
    session = settings.Session()

    error_response = check_dag_exists(session, dag_id)
    if error_response:
        return error_response

    execution_date = datetime.now()

    run_id = None
    json_params = request.get_json()
    if json_params and 'run_id' in json_params:
        run_id = json_params['run_id']
    if not run_id:
        run_id = 'external_trigger_' + execution_date.isoformat()

    trigger = DagRun(
        dag_id=dag_id,
        run_id=run_id,
        state=State.RUNNING,
        execution_date=execution_date,
        external_trigger=True)
    session.add(trigger)
    session.commit()

    return jsonify(dag_id=dag_id, run_id=run_id)


@TriggerBlueprint.route('/<dag_id>', methods=['GET'])
@csrf.exempt
def get_dag_runs(dag_id):
    """
    .. http:get:: /trigger/<dag_id>

        Get the run_ids for a dag_id, ordered by execution date

        **Example request**:

        .. sourcecode:: http

            GET /trigger/make_fit
            Host: localhost:7357

        **Example response**:

        .. sourcecode:: http

            HTTP/1.1 200 OK
            Content-Type: application/json

            {
              "dag_id": "daily_processing",
              "run_ids": ["my_special_run", "normal_run_17"]
            }
    """
    session = settings.Session()

    error_response = check_dag_exists(session, dag_id)
    if error_response:
        return error_response

    dag_runs = session.query(DagRun).filter(DagRun.dag_id == dag_id).order_by(DagRun.execution_date).all()
    run_ids = [dag_run.run_id for dag_run in dag_runs]

    return jsonify(dag_id=dag_id, run_ids=run_ids)


@TriggerBlueprint.route('/<dag_id>/<run_id>', methods=['GET'])
@csrf.exempt
def dag_run_status(dag_id, run_id):
    """
    .. http:get:: /trigger/<dag_id>/<run_id>

        Gets the status of a dag run.
        Possible states are: running, success, failed

        **Example request**:

        .. sourcecode:: http

            GET /trigger/make_fit/my_special_run
            Host: localhost:7357

        **Example response**:

        .. sourcecode:: http

            HTTP/1.1 200 OK
            Content-Type: application/json

            {
              "dag_id": "daily_processing",
              "run_id": "my_special_run",
              "state": "running",
              "execution_date": "2016-06-27T15:32:57"
            }
    """
    session = settings.Session()

    error_response = check_dag_exists(session, dag_id)
    if error_response:
        return error_response

    try:
        dag_run = session.query(DagRun).filter(and_(DagRun.dag_id == dag_id, DagRun.run_id == run_id)).one()
    except NoResultFound:
        return Response('RunId {} does not exist for Dag {}'.format(run_id, dag_id), httplib.BAD_REQUEST)

    time_format = "%Y-%m-%dT%H:%M:%S"
    return jsonify(
        dag_id=dag_id,
        run_id=run_id,
        state=dag_run.state,
        execution_date=dag_run.execution_date.strftime(time_format)
    )
