# -*- coding: utf-8 -*-
import json
import requests
import time

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException, AirflowSkipException
from ..hooks.dbt_cloud_hook import DbtCloudHook


SUCCESSFUL_STATUSES = ['success', 'pass']


class DbtCloudCheckModelResultOperator(BaseOperator):

    template_fields = ['run_id']

    @apply_defaults
    def __init__(self, dbt_cloud_conn_id=None, dbt_cloud_run_id=None, model_names=None, ensure_models_ran=True, *args, **kwargs):
        super(DbtCloudCheckJobResultOperator, self).__init__(*args, **kwargs)

        if dbt_cloud_conn_id is None:
            raise AirflowException('No valid dbt Cloud connection ID was supplied.')
        if dbt_cloud_run_id is None:
            raise AirflowException('No dbt Cloud run_id was supplied.')
        if model_names is None:
            raise AirflowException('No model names supplied.')

        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.dbt_cloud_run_id = dbt_cloud_run_id
        if isinstance(model_names, str):
            model_names = [model_names]
        self.model_names = model_names
        self.ensure_models_ran = ensure_models_ran

    def _find_test_dependencies_for_model_id(self, model_id, manifest):
        tests = []
        for node, values in manifest['nodes'].items():
            if values['resource_type'] != 'test':
                continue
            for dependency in values['depends_on']['nodes']:
                if dependency == model_id:
                    tests.append(node)
        return tests

    def _find_model_id_from_name(self, model_name, manifest):
        models = manifest['nodes'].values()
        for model in models:
            if model['name'] == model_name:
                return model['unique_id']

    def _check_that_model_passed(self, model_name, manifest, run_results):
        model_id = self._find_model_id_from_name(model_name, manifest)
        tests = self._find_test_dependencies_for_model_id(model_id, manifest)
        all_dependencies = [model_id] + tests
        self.log.info(f'Checking all dependencies for {model_name}: {all_dependencies}')

        ran_model = False
        for result in run_results:
            if result['unique_id'] == model_id:
                ran_model = True
            if result['unique_id'] in all_dependencies:
                if result['status'] not in SUCCESSFUL_STATUSES:
                    raise Exception(f'Dependency {result["unique_id"]} did not pass, status: {result["status"]}!')

        if not ran_model and ensure_models_ran:
            raise Exception(f'Model {model_id} was not run!')

    def execute(self, **kwargs):
        dbt_cloud_hook = DbtCloudHook(dbt_cloud_conn_id=self.dbt_cloud_conn_id)
        manifest = dbt_cloud_hook.get_run_manifest(self.dbt_cloud_run_id)
        run_results = dbt_cloud_hook.get_all_run_results(self.dbt_cloud_run_id)

        for model in self.model_names:
            self._check_that_model_passed(model, manifest, run_results)
        
