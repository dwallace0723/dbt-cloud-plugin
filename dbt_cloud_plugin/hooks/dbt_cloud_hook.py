from dbt_cloud_plugin.dbt_cloud.dbt_cloud import DbtCloud
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

class RunStatus:
    queued = 1
    dequeued = 2
    running = 3
    success = 10
    error = 20
    cancelled = 30

    LOOKUP = {
        queued: 'Queued',
        dequeued: 'Queued',
        running: 'Running',
        success: 'Success',
        error: 'Error',
        cancelled: 'Cancelled',
    }

    @classmethod
    def lookup(cls, status):
        return cls.LOOKUP.get(status, 'Unknown')

class DbtCloudHook(BaseHook):
    """
    Interact with dbt Cloud.
    """

    def __init__(self, dbt_cloud_conn_id):
        self.dbt_cloud_conn_id = dbt_cloud_conn_id

    def get_conn(self):
        conn = self.get_connection(self.dbt_cloud_conn_id)
        if 'dbt_cloud_api_token' in conn.extra_dejson:
            dbt_cloud_api_token = conn.extra_dejson['dbt_cloud_api_token']
        else:
            raise AirflowException('No dbt Cloud API Token was supplied in dbt Cloud connection.')
        if 'dbt_cloud_account_id' in conn.extra_dejson:
            dbt_cloud_account_id = conn.extra_dejson['dbt_cloud_account_id']
        else:
            raise AirflowException('No dbt Cloud Account ID was supplied in dbt Cloud connection.')

        return DbtCloud(dbt_cloud_account_id, dbt_cloud_api_token)

    def get_run_status(self, run_id):
        """
        Return the status of an dbt cloud run.
        """

        dbt_cloud = self.get_conn()
        run = dbt_cloud.try_get_run(run_id=run_id)
        status_name = RunStatus.lookup(run['status'])
        return status_name
