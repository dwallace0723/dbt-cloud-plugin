# -*- coding: utf-8 -*-
import json
import requests
import time

from airflow.models import BaseOperator
from dbt_cloud_plugin.hooks.dbt_cloud_hook import DbtCloudHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

class DbtCloudRunJobOperator(BaseOperator):
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
                 dbt_cloud_conn_id=None,
                 project_id=None,
                 job_name=None,
                 *args, **kwargs):
        super(DbtCloudRunJobOperator, self).__init__(*args, **kwargs)

        if dbt_cloud_conn_id is None:
            raise AirflowException('No valid dbt cloud connection ID was supplied.')

        if project_id is None:
            raise AirflowException('No valid project id was supplied.')

        if job_name is None:
            raise AirflowException('No job name was supplied.')

        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.project_id = project_id
        self.job_name = job_name

    def execute(self, **kwargs):
        """
        Hits the dbt Cloud API and blocks until the job succeeds.
        """

        self.log.info('Attempting to trigger a run of dbt Cloud job: {}'.format(self.job_name))

        try:
            dbt_cloud_hook = DbtCloudHook(dbt_cloud_conn_id=self.dbt_cloud_conn_id)
            dbt_cloud = dbt_cloud_hook.get_conn()
            trigger_resp = dbt_cloud.run_job(self.project_id, self.job_name)
            self.log.info('Triggered Job ID {}'.format(trigger_resp['id']))
        except RuntimeError as e:
            raise AirflowException("Error while triggering job {}: {}".format(self.job_name, e))

        return trigger_resp['id']
