"""
dbt Cloud Hourly DAG
"""

from airflow import DAG, utils
from airflow.operators import DbtCloudRunJobOperator
from airflow.sensors import DbtCloudJobSensor
from datetime import datetime,timedelta
import pendulum

local_tz = pendulum.timezone("America/Los_Angeles")

PROJECT_ID = your_dbt_cloud_project_id

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

# Run the cashflow DAG through dbt cloud.
run_dbt_cloud_job = DbtCloudRunJobOperator(
    task_id='run_dbt_cloud_job',
    dbt_cloud_conn_id='dbt_cloud',
    project_id=PROJECT_ID,
    job_name='Hourly Job',
    dag=dag)

# Watch the progress of the cashflow DAG.
watch_dbt_cloud_job = DbtCloudJobSensor(
    task_id='watch_dbt_cloud_job',
    dbt_cloud_conn_id='dbt_cloud',
    project_id=PROJECT_ID,
    job_id="{{ task_instance.xcom_pull('run_dbt_cloud_job', key='return_value') }}",
    sla=timedelta(minutes=45),
    dag=dag)

watch_dbt_cloud_job.set_upstream(run_dbt_cloud_job)
