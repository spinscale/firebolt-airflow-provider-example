import time
import airflow
from airflow.models import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from firebolt_provider.operators.firebolt \
  import FireboltOperator, FireboltStartEngineOperator, FireboltStopEngineOperator
from firebolt.client.auth import UsernamePassword
from firebolt.db import connect
import datetime
import os

default_args = {
    'owner': '',
    'start_date': airflow.utils.dates.days_ago(1)
}

### Function to connect to Firebolt
def connection_params(conn_opp, field):
    connector = FireboltOperator(
        firebolt_conn_id=conn_opp, sql="", task_id="CONNECT")
    return connector.get_db_hook()._get_conn_params()[field]

FIREBOLT_CONN_ID = 'your-configured-connection-id'
# Make sure this is a read-write engine
FIREBOLT_ENGINE_NAME = 'a-read-write-engine'

dag = DAG('firebolt_update_github_events',
          default_args=default_args,
          schedule_interval=None,
          catchup=False)

task_start_engine = FireboltStartEngineOperator(
    dag=dag,
    task_id="github_events_start_firebolt_engine",
    firebolt_conn_id=FIREBOLT_CONN_ID,
    engine_name=FIREBOLT_ENGINE_NAME)

# Use this instead of the FireboltOperator as one initial query needs to run
# that acts as a filter
# Requires a setup of the connection, but good otherwise
def ingest_new_data():
    # setup firebolt connection
    airflow_conn = BaseHook.get_connection(FIREBOLT_CONN_ID)
    database = airflow_conn.schema

    connection = connect(engine_name=FIREBOLT_ENGINE_NAME,
                         api_endpoint='api.app.firebolt.io',
                         database=database,
                         auth=UsernamePassword(airflow_conn.login, airflow_conn.password))

    cursor = connection.cursor()
    print('Setup Firebolt connection')

    # retrieve last update
    cursor.execute("SELECT max(created_at)::date FROM gharchive;")
    start_date = cursor.fetchone()[0]
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    delta = end_date - start_date
    print('Last update: %s, end date: %s, delta %s' % (start_date, end_date, delta))

    # no delta, no work
    if delta.days > 0:
        filter_days = []
        for i in range(delta.days + 1):
            day = start_date + datetime.timedelta(days=i)
            filter_days.append("source_file_name LIKE 'gharchive/%04d/%02d/%02d/%%'" % (day.year, day.month, day.day))

        sql_filter = '(' + ' or \n'.join(filter_days) + ')'

        directory = os.path.dirname(os.path.abspath(__file__))
        query_file_path = os.path.join(directory, "insert-into-gharchive.sql")

        with open(query_file_path, 'r') as file:
            raw_sql_query = file.read()
            query = raw_sql_query % sql_filter
            print('About to run %s\n' % query)
            cursor.execute(query)

    # close connection
    cursor.close
    connection.close
    print('Closed Firebolt connection')

task_update_all_data = PythonOperator(task_id='github_events_ingest_new_data', python_callable=ingest_new_data, dag=dag)

# leave this commented out to prevent stopping of the engine!
#task_stop_engine = FireboltStopEngineOperator(
#    dag=dag,
#    task_id="github_events_stop_firebolt_engine",
#    firebolt_conn_id=FIREBOLT_CONN_ID,
#    engine_name=FIREBOLT_ENGINE_NAME)

# task dependencies
task_start_engine >> task_update_all_data
#task_start_engine >> task_update_all_data >> task_stop_engine
