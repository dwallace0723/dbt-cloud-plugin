# -*- coding: utf-8 -*-
import json
import requests
import time

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from ..hooks.dbt_cloud_hook import DbtCloudHook

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
                 job_name=None,
                 git_branch=None,
                 schema_override=None,
                 target_name_override=None,
                 steps_override=None,
                 environment_id=None,
                 *args, **kwargs):
        super(DbtCloudRunJobOperator, self).__init__(*args, **kwargs)

        if dbt_cloud_conn_id is None:
            raise AirflowException('No valid dbt cloud connection ID was supplied.')

        if job_name is None:
            raise AirflowException('No job name was supplied.')

        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.job_name = job_name
        self.git_branch = git_branch
        self.schema_override = schema_override
        self.target_name_override = target_name_override
        self.steps_override = steps_override
        self.environment_id = environment_id

    def execute(self, **kwargs):

        self.log.info('Attempting to trigger a run of dbt cloud job: {}'.format(self.job_name))

        try:
            dbt_cloud_hook = DbtCloudHook(dbt_cloud_conn_id=self.dbt_cloud_conn_id)
            trigger_resp = dbt_cloud_hook.run_job(
                self.job_name,
                git_branch=self.git_branch,
                schema_override=self.schema_override,
                target_name_override=self.target_name_override,
                steps_override=self.steps_override,
                environment_id=self.environment_id
            )
            self.log.info('Triggered Run ID {}'.format(trigger_resp['id']))
        except RuntimeError as e:
            raise AirflowException("Error while triggering job {}: {}".format(self.job_name, e))

        return trigger_resp['id']
