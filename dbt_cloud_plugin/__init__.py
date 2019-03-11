from airflow.plugins_manager import AirflowPlugin
from dbt_cloud_plugin.hooks.dbt_cloud_hook import DbtCloudHook
from dbt_cloud_plugin.operators.dbt_cloud_run_job_operator import DbtCloudRunJobOperator
from dbt_cloud_plugin.sensors.dbt_cloud_run_sensor import DbtCloudRunSensor

class DbtCloudPlugin(AirflowPlugin):
    name = "dbt_cloud_plugin"
    operators = [DbtCloudRunJobOperator]
    hooks = [DbtCloudHook]
    sensors = [DbtCloudRunSensor]
