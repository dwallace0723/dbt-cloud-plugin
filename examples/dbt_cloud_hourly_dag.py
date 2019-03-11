"""
dbt Cloud Hourly DAG
"""

from airflow import DAG, utils
from airflow.operators import DbtCloudRunJobOperator
from airflow.sensors import DbtCloudRunSensor
from datetime import datetime,timedelta
import pendulum

local_tz = pendulum.timezone("America/Los_Angeles")

default_args = {
    'owner': 'dwall',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 8, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

dag = DAG('dbt_cloud_hourly_dag', concurrency=1, max_active_runs=1, catchup=False, schedule_interval='0 * * * *', default_args=default_args)
dag.doc_md = __doc__

# Run hourly DAG through dbt cloud.
run_dbt_cloud_job = DbtCloudRunJobOperator(
    task_id='run_dbt_cloud_job',
    dbt_cloud_conn_id='dbt_cloud',
    job_name='Hourly Job',
    dag=dag)

# Watch the progress of the DAG.
watch_dbt_cloud_job = DbtCloudRunSensor(
    task_id='watch_dbt_cloud_job',
    dbt_cloud_conn_id='dbt_cloud',
    job_id="{{ task_instance.xcom_pull(task_ids='run_dbt_cloud_job', dag_id='dbt_cloud_hourly_dag', key='return_value') }}",
    sla=timedelta(minutes=45),
    dag=dag)

watch_dbt_cloud_job.set_upstream(run_dbt_cloud_job)
