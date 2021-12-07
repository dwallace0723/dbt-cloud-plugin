# -*- coding: utf-8 -*-
import json
import requests
import time

from airflow.exceptions import AirflowException


class DbtCloud(object):
    """
    Class for interacting with the dbt Cloud API
    * :py:meth:`list_jobs` - list all Jobs for the specified Account ID
    * :py:meth:`get_run` - Get information about a specified Run ID
    * :py:meth:`trigger_job_run` - Trigger a Run for a specified Job ID
    * :py:meth: `try_get_run` - Attempts to get information about a specific Run ID for up to max_tries
    * :py:meth: `run_job` - Triggers a run for a job using the job name
    """

    def __init__(self, account_id, api_token):
        self.account_id = account_id
        self.api_token = api_token
        self.api_base = 'https://cloud.getdbt.com/api/v2'

    def _get(self, url_suffix):
        url = self.api_base + url_suffix
        headers = {'Authorization': 'Token %s' % self.api_token}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise RuntimeError(response.content)

    def _post(self, url_suffix, data=None):
        url = self.api_base + url_suffix
        headers = {'Authorization': 'token %s' % self.api_token}
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise RuntimeError(response.content)

    def list_jobs(self, environment_id=None):
        jobs = self._get('/accounts/%s/jobs/' % self.account_id).get('data')
        if environment_id is not None:
            return [j for j in jobs if str(j['environment_id']) == str(environment_id)]
        else:
            return jobs

    def get_run(self, run_id):
        return self._get('/accounts/%s/runs/%s/' % (self.account_id, run_id)).get('data')

    def trigger_job_run(self, job_id, data=None):
        return self._post(url_suffix='/accounts/%s/jobs/%s/run/' % (self.account_id, job_id), data=data).get('data')

    def try_get_run(self, run_id, max_tries=3):
        for i in range(max_tries):
            try:
                run = self.get_run(run_id)
                return run
            except RuntimeError as e:
                print("Encountered a runtime error while fetching status for {}".format(run_id))
                time.sleep(10)

        raise RuntimeError("Too many failures ({}) while querying for run status".format(run_id))

    def run_job(self, job_name, data=None, environment_id=None):
        jobs = self.list_jobs(environment_id=environment_id)

        job_matches = [j for j in jobs if j['name'] == job_name]

        if len(job_matches) != 1:
            raise AirflowException("{} jobs found for {}".format(len(job_matches), job_name))

        job_def = job_matches[0]
        trigger_resp = self.trigger_job_run(job_id=job_def['id'], data=data)
        return trigger_resp
