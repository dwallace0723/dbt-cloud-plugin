from ..dbt_cloud.dbt_cloud import DbtCloud
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

    def _get_conn_extra(self):
        conn = self.get_connection(self.dbt_cloud_conn_id).extra_dejson
        config = {}
        if 'git_branch' in conn:
            config['git_branch'] = conn['git_branch']
        if 'schema_override' in conn:
            config['schema_override'] = conn['schema_override']
        if 'target_name_override' in conn:
            config['target_name_override'] = conn['target_name_override']
        if 'environment_id' in conn:
            config['environment_id'] = conn['environment_id']

        return config

    def get_run_status(self, run_id):
        """
        Return the status of an dbt cloud run.
        """

        dbt_cloud = self.get_conn()
        run = dbt_cloud.try_get_run(run_id=run_id)
        status_name = RunStatus.lookup(run['status'])
        return status_name

    def run_job(self, job_name, git_branch=None, schema_override=None,
                target_name_override=None, steps_override=None, environment_id=None):
        dbt_cloud = self.get_conn()
        extra = self._get_conn_extra()

        data = {'cause': 'Kicked off via Airflow'}
        # add optional settings
        if git_branch or extra.get('git_branch', None):
            data['git_branch'] = git_branch or extra.get('git_branch', None)
        if schema_override or extra.get('schema_override', None):
            data['schema_override'] = schema_override or extra.get('schema_override', None)
        if target_name_override or extra.get('target_name_override', None):
            data['target_name_override'] = target_name_override or extra.get('target_name_override', None)
        if steps_override:
            data['steps_override'] = steps_override

        # get environment
        environment_id = environment_id or extra.get('environment_id', None)

        self.log.info(f'Triggering job {job_name} with data {data}')

        return dbt_cloud.run_job(job_name, data=data, environment_id=environment_id)
