# -*- coding: utf-8 -*-
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from dbt_cloud_plugin.hooks.dbt_cloud_hook import DbtCloudHook


class DbtCloudRunJobOperator(BaseOperator):
    """
    Operator to run a dbt cloud job.
    :param dbt_cloud_conn_id: dbt Cloud connection ID.
    :type dbt_cloud_conn_id: string
    :param job_name: dbt Cloud job name.
    :type job_name: string
    """

    @apply_defaults
    def __init__(self,
                 dbt_cloud_conn_id=None,
                 job_name=None,
                 *args, **kwargs):
        super(DbtCloudRunJobOperator, self).__init__(*args, **kwargs)

        if dbt_cloud_conn_id is None:
            raise AirflowException('No valid dbt Cloud '
                                   'connection ID was supplied.')

        if job_name is None:
            raise AirflowException('No job name was supplied.')

        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.job_name = job_name

    def execute(self, **kwargs):

        self.log.info(
            f'Attempting to trigger a run of dbt Cloud job: {self.job_name}'
        )

        try:
            dbt_cloud_hook = DbtCloudHook(dbt_cloud_conn_id=self.dbt_cloud_conn_id)
            dbt_cloud = dbt_cloud_hook.get_conn()

            data = {'cause': 'Kicked off via Airflow'}
            trigger_resp = dbt_cloud.run_job(self.job_name, data=data)
            triggered_run_id = trigger_resp['id']

            self.log.info(f'Triggered Run ID {triggered_run_id}')

        except RuntimeError as e:
            raise AirflowException(
                f'Error while triggering job {self.job_name}: {e}'
            )

        return triggered_run_id
