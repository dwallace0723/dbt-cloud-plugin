# -*- coding: utf-8 -*-
import time

from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException, AirflowSkipException
from ..hooks.dbt_cloud_hook import DbtCloudHook
from ..operators.dbt_cloud_run_job_operator import DbtCloudRunJobOperator
from ..helpers import DbtCloudRunException


class DbtCloudRunAndWatchJobOperator(DbtCloudRunJobOperator):
    """
    Operator to run a dbt cloud job.
    :param dbt_cloud_conn_id: dbt Cloud connection ID.
    :type dbt_cloud_conn_id: string
    :param project_id: dbt Cloud project ID.
    :type project_id: int
    :param job_name: dbt Cloud job name.
    :type job_name: string
    """

    @apply_defaults
    def __init__(self,
                 poke_interval=60,
                 timeout=60 * 60 * 24,
                 soft_fail=False,
                 *args, **kwargs):
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.soft_fail = soft_fail
        super(DbtCloudRunAndWatchJobOperator, self).__init__(*args, **kwargs)

    def execute(self, **kwargs):
        response = super(DbtCloudRunAndWatchJobOperator, self).execute(**kwargs)
        run_id = response['id']

        self.account_id = response['job']['account_id']
        self.project_id = response['job']['project_id']
        self.environment_id = response['job']['environment_id']
        
        # basically copy-pasting the Sensor code
        self.log.info(f'Starting poke for job {run_id}')
        try_number = 1
        started_at = time.monotonic()

        def run_duration():
            nonlocal started_at
            return time.monotonic() - started_at

        while not self.poke(run_id):
            if run_duration() > self.timeout:
                if self.soft_fail:
                    raise AirflowSkipException(f'Time is out!')
                else:
                    raise AirflowException(f'Time is out!')
            else:
                time.sleep(self.poke_interval)
                try_number += 1
        self.log.info('Success criteria met. Exiting.')

    def poke(self, run_id):
        self.log.info('Sensor checking state of dbt cloud run ID: %s', run_id)
        dbt_cloud_hook = DbtCloudHook(dbt_cloud_conn_id=self.dbt_cloud_conn_id)
        run_status = dbt_cloud_hook.get_run_status(run_id=run_id)
        self.log.info('State of Run ID {}: {}'.format(run_id, run_status))

        if run_status.strip() == 'Cancelled':
            raise AirflowException(f'dbt cloud Run ID {run_id} Cancelled.')
        
        elif run_status.strip() == 'Error':
            run_results = dbt_cloud_hook.get_all_run_results(run_id=run_id)
            manifest = dbt_cloud_hook.get_run_manifest(run_id=run_id)

            errors = {}
            fail_states = {result['unique_id'] for result in run_results if result['status'] in ['error', 'failure']}
            for unique_id in fail_states:
                errors[unique_id] = {
                    'tags': manifest['nodes'][unique_id]['tags'], 
                    'resource_type': manifest['nodes'][unique_id]['resource_type'],
                    'depends_on': manifest['nodes'][unique_id]['depends_on']['nodes'],
                    'parent_models': {model: manifest['nodes'][model]['tags'] for model in manifest['nodes'][unique_id]['depends_on']['nodes']}
                }

            raise DbtCloudRunException(
                dbt_cloud_run_id=run_id,
                dbt_cloud_account_id=self.account_id,
                dbt_cloud_project_id=self.project_id,
                error_message=f'dbt cloud Run ID {run_id} Failed.', 
                dbt_errors_dict=errors
            )
            
        elif run_status.strip() == 'Success':
            return True
        
        else:
            return False
