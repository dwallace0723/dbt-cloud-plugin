import unittest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException
from dbt_cloud_plugin.operators.dbt_cloud_check_model_result_operator import DbtCloudCheckModelResultOperator, DbtModelFailedException, DbtModelNotRunException

class TestDbtCloudCheckModelResultOperator(unittest.TestCase):

    def setUp(self):
        self.operator = DbtCloudCheckModelResultOperator(
            task_id='test_task',
            dbt_cloud_conn_id='dbt_cloud_conn',
            dbt_cloud_run_id='run_id',
            model_names=['model1', 'model2'],
        )

    def test_init_with_invalid_run_id(self):
        with self.assertRaises(AirflowException):
            DbtCloudCheckModelResultOperator(
                task_id='test_task',
                dbt_cloud_conn_id='dbt_cloud_conn',
                dbt_cloud_run_id=None,
                model_names=['model1'],
            )

    def test_init_with_invalid_model_names(self):
        with self.assertRaises(AirflowException):
            DbtCloudCheckModelResultOperator(
                task_id='test_task',
                dbt_cloud_conn_id='dbt_cloud_conn',
                dbt_cloud_run_id='run_id',
                model_names=None,
            )

    def test_find_test_dependencies_for_model_id(self):
        manifest = {
            'nodes': {
                'model_id1': {'resource_type': 'model', 'depends_on': {'nodes': []}},
                'model_id2': {'resource_type': 'model', 'depends_on': {'nodes': []}},
                'test_id1': {'resource_type': 'test', 'depends_on': {'nodes': ['model_id1']}},
                'test_id2': {'resource_type': 'test', 'depends_on': {'nodes': ['model_id2']}},
            }
        }
        result = self.operator._find_test_dependencies_for_model_id('model_id1', manifest)
        self.assertEqual(result, ['test_id1'])

    def test_find_model_id_from_name(self):
        manifest = {
            'nodes': {
                'model_id1': {'name': 'model1', 'unique_id': 'model_id1'},
                'model_id2': {'name': 'model2', 'unique_id': 'model_id2'},
            }
        }
        result = self.operator._find_model_id_from_name('model2', manifest)
        self.assertEqual(result, 'model_id2')

    def test_check_that_model_passed(self):
        manifest = {
            'nodes': {
                'model_id1': {'name': 'model1', 'unique_id': 'model_id1', 'resource_type': 'model'},
            }
        }
        run_results = [{'unique_id': 'model_id1', 'status': 'success'}]
        self.operator._check_that_model_passed('model1', manifest, run_results)

        run_results = [{'unique_id': 'model_id1', 'status': 'pass'}]
        self.operator._check_that_model_passed('model1', manifest, run_results)
        
    def test_check_that_model_passed_test_dependency(self):
        manifest = {
            'nodes': {
                'model_id1': {'name': 'model1', 'unique_id': 'model_id1', 'resource_type': 'model'},
                'test_id1': {'name': 'test1', 'unique_id': 'test_id1', 'resource_type': 'test',
                          'depends_on': {'nodes': ['model_id1']}}
            }
        }
        run_results = [{'unique_id': 'model_id1', 'status': 'success'}]
        self.operator._check_that_model_passed('model1', manifest, run_results)

        run_results = [{'unique_id': 'model_id1', 'status': 'pass'}]
        self.operator._check_that_model_passed('model1', manifest, run_results)

        run_results = [{'unique_id': 'model_id1', 'status': 'pass'},
                       {'unique_id': 'test_id1', 'status': 'failed'}]
        with self.assertRaises(DbtModelFailedException):
            self.operator._check_that_model_passed('model1', manifest, run_results)

    def test_check_that_model_failed(self):
        manifest = {
            'nodes': {
                'model_id1': {'name': 'model1', 'unique_id': 'model_id1', 'resource_type': 'model'},
            }
        }
        run_results = [{'unique_id': 'model_id1', 'status': 'failed'}]
        with self.assertRaises(DbtModelFailedException):
            self.operator._check_that_model_passed('model1', manifest, run_results)

    def test_check_that_model_did_not_run_when_ensuring_models_ran(self):
        manifest = {
            'nodes': {
                'model_id1': {'name': 'model1', 'unique_id': 'model_id1', 'resource_type': 'model'},
            }
        }
        run_results = []
        with self.assertRaises(DbtModelNotRunException):
            self.operator._check_that_model_passed('model1', manifest, run_results)

    def test_ignore_that_model_did_not_run_when_not_ensuring_models_ran(self):
        manifest = {
            'nodes': {
                'model_id1': {'name': 'model1', 'unique_id': 'model_id1', 'resource_type': 'model'},
            }
        }
        run_results = []
        self.operator.ensure_models_ran = False
        self.operator._check_that_model_passed('model1', manifest, run_results)

    @patch('dbt_cloud_plugin.operators.dbt_cloud_check_model_result_operator.DbtCloudHook')
    def test_execute(self, mock_hook_class):
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_run_manifest.return_value = {}
        mock_hook.get_all_run_results.return_value = []
        self.operator._check_that_model_passed = MagicMock()
        self.operator._check_that_model_passed.return_value = None

        self.operator.execute()

        mock_hook.get_run_manifest.assert_called_once_with('run_id')
        mock_hook.get_all_run_results.assert_called_once_with('run_id')
        self.operator._check_that_model_passed.assert_any_call('model1', {}, [])
        self.operator._check_that_model_passed.assert_any_call('model2', {}, [])

if __name__ == '__main__':
    unittest.main()
