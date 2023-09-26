# -*- coding: utf-8 -*-
import json
import requests
import time

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException, AirflowSkipException
from ..hooks.dbt_cloud_hook import DbtCloudHook


SUCCESSFUL_STATUSES = ['success', 'pass']


class DbtModelException(Exception):
    pass


class DbtModelFailedException(DbtModelException):
    pass


class DbtModelNotRunException(DbtModelException):
    pass


class DbtCloudCheckModelResultOperator(BaseOperator):
    """
    Check the results of a dbt Cloud job to see whether the model(s) you
    care about ran successfully. Useful if you have a large dbt Cloud job,
    but each of your downstream tasks only requires a small subset of models
    to succeed.

    :param dbt_cloud_run_id: Run ID of a finished dbt Cloud job. Note that
        this task must not start running until the dbt Cloud job has completed,
        otherwise it will error out. See DbtCloudRunAndWatchJobOperator or
        DbtCloudRunSensor.
    :type dbt_cloud_run_id: str or int
    :param model_names: A single model name or list of model names to check.
        Tests and snapshot names also work. Note this must be the /name/, not the
        node ID. In addition to the model name(s) supplied, all of the tests for
        that model will also be checked (though unlike the model itself, they need
        not have been run).
    :type model_names: str or list[str]
    :param dbt_cloud_conn_id: dbt Cloud connection ID
    :type dbt_cloud_conn_id: str
    :param ensure_models_ran: Whether to ensure all of the model_names were actually
        executed in this run. Defaults to True to avoid accidentally mistyping the
        model name, negating the value of this check.
    :type ensure_models_ran: bool, default True
    """

    template_fields = ['dbt_cloud_run_id']

    @apply_defaults
    def __init__(self, dbt_cloud_run_id=None, model_names=None, ensure_models_ran=True, dbt_cloud_conn_id='dbt_default', *args, **kwargs):
        super(DbtCloudCheckModelResultOperator, self).__init__(*args, **kwargs)

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
                    raise DbtModelFailedException(f'Dependency {result["unique_id"]} did not pass, status: {result["status"]}!')

        if not ran_model and self.ensure_models_ran:
            raise DbtModelNotRunException(f'Model {model_id} was not run!')

    def execute(self, **kwargs):
        dbt_cloud_hook = DbtCloudHook(dbt_cloud_conn_id=self.dbt_cloud_conn_id)
        manifest = dbt_cloud_hook.get_run_manifest(self.dbt_cloud_run_id)
        run_results = dbt_cloud_hook.get_all_run_results(self.dbt_cloud_run_id)

        for model in self.model_names:
            self._check_that_model_passed(model, manifest, run_results)
        
