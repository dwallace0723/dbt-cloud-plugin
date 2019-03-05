# -*- coding: utf-8 -*-
import json
import requests
import time

class DbtCloud(object):
    """
    Class for interacting with the dbt Cloud API
    * :py:meth:`list_projects` - lists all projects under the account specified when instantiating the DbtCloud object
    * :py:meth:`get_project' - returns details of a single project id
    * :py:meth:`list_job_definitions` - lists all job definitions for the specified project id
    * :py:meth:`get_job_definition` - (not implemented) returns details of a single job definition
    * :py:meth:`list_job_runs` - (not implemented) lists all job runs for the specified job id
    * :py:meth:`get_job_run` - (not implemented) returns details of a single job run
    * :py:meth:`trigger_job_run` - triggers an execution of a job definition
    """

    def __init__(self, account_id, api_token):
        self.account_id = account_id
        self.api_token = api_token
        self.api_base = 'https://cloud.getdbt.com/api/v1'

    def _get(self, url_suffix):
        url = self.api_base + url_suffix
        headers = {'Authorization': 'Token %s' % self.api_token}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise RuntimeError(response.content)

    def _post(self, url_suffix):
        url = self.api_base + url_suffix
        headers = {'Authorization': 'token %s' % self.api_token}
        response = requests.post(url, headers=headers)
        if response.status_code == 201:
            return json.loads(response.content)
        else:
            raise RuntimeError(response.content)

    def list_projects(self):
        return self._get('/accounts/%s/projects/' % self.account_id)['data']

    def get_project(self, project_id):
        return self._get('/accounts/%s/projects/%s/' % (self.account_id, project_id))['data']

    def list_job_definitions(self, project_id):
        return self._get('/accounts/%s/projects/%s/definitions/' % (self.account_id, project_id))

    def get_job_run(self, project_id, job_id):
        return self._get('/accounts/%s/projects/%s/runs/%s/' % (self.account_id, project_id, job_id))

    def trigger_job_run(self, project_id, definition_id):
        return self._post('/accounts/%s/projects/%s/definitions/%s/runs/' % (self.account_id, project_id, definition_id))

    def try_get_job_run(self, project_id, job_id, max_tries=3):
        for i in range(max_tries):
            try:
                job = self.get_job_run(project_id, job_id)
                return job
            except RuntimeError as e:
                print("Encountered a runtime error while fetching status for {}".format(job_id))
                time.sleep(10)

        raise RuntimeError("Too many failures ({}) while querying for job status".format(job_id))

    def block_until_complete(self, project_id, job_id):
        query_sleep = 30
        while True:
            job = self.try_get_job_run(project_id, job_id)
            status_name = RunStatus.lookup(job['status'])

            print("JOB: {}, STATUS: {}".format(job_id, status_name))
            if status_name in ['Success', 'Error', 'Cancelled']:
                return job
            else:
                time.sleep(query_sleep)

    def run_job(self, project_id, job_name):
        definitions = self.list_job_definitions(project_id)

        job_defs = [d for d in definitions if d['name'] == job_name]

        if len(job_defs) != 1:
            raise AirflowException("{} jobs found for {}".format(len(job_defs), job_name))

        job_def = job_defs[0]
        trigger_resp = self.trigger_job_run(project_id, job_def['id'])
        return trigger_resp
