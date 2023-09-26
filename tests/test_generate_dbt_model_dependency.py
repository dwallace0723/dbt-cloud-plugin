import unittest
from unittest.mock import Mock
from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from dbt_cloud_plugin.helpers import generate_dbt_model_dependency
from dbt_cloud_plugin.operators.dbt_cloud_run_and_watch_job_operator import DbtCloudRunAndWatchJobOperator
from dbt_cloud_plugin.operators.dbt_cloud_check_model_result_operator import DbtCloudCheckModelResultOperator


class TestGenerateDbtModelDependency(unittest.TestCase):

    def test_generate_dbt_model_dependency_with_single_task(self):
        # Create a minimal DAG for testing
        dag = DAG(dag_id='test_dag', start_date=datetime(2023, 1, 1), catchup=False)
        with dag:
            dbt_job_task = DbtCloudRunAndWatchJobOperator(task_id='dbt_job_task', dbt_cloud_conn_id='dbt_default', job_name='test job')

            task1 = DummyOperator(task_id='task1')

            generate_dbt_model_dependency(dbt_job_task, task1, ['model1', 'model2'], ensure_models_ran=True)

        self.assertEqual(len(dag.tasks), 3)
        # find the generated task
        for task in dag.tasks:
            if isinstance(task, DbtCloudCheckModelResultOperator):
                result = task
        
        # Verify the generated dependencies
        self.assertEqual(result.task_id, 'check_dbt_model_results__dbt_job_task__model1__model2')
        self.assertIsInstance(result, DbtCloudCheckModelResultOperator)
        self.assertEqual(result.dbt_cloud_conn_id, dbt_job_task.dbt_cloud_conn_id)
        self.assertEqual(result.ensure_models_ran, True)
        self.assertEqual(result.trigger_rule, 'all_done')
        self.assertEqual(result.retries, 0)
        self.assertIn(dbt_job_task, result.get_flat_relatives(upstream=True))
        self.assertIn(task1, result.get_flat_relatives(upstream=False))

    def test_generate_dbt_model_dependency_with_list(self):
        # Create a minimal DAG for testing
        dag = DAG(dag_id='test_dag', start_date=datetime(2023, 1, 1), catchup=False)
        with dag:
            dbt_job_task = DbtCloudRunAndWatchJobOperator(task_id='dbt_job_task', dbt_cloud_conn_id='dbt_default', job_name='test job')

            task1 = DummyOperator(task_id='task1')
            task2 = DummyOperator(task_id='task2')
            downstream_tasks = [task1, task2]

            generate_dbt_model_dependency(dbt_job_task, downstream_tasks, ['model1', 'model2'], ensure_models_ran=True)

        self.assertEqual(len(dag.tasks), 4)
        # find the generated task
        for task in dag.tasks:
            if isinstance(task, DbtCloudCheckModelResultOperator):
                result = task
        
        # Verify the generated dependencies
        self.assertEqual(result.task_id, 'check_dbt_model_results__dbt_job_task__model1__model2')
        self.assertIsInstance(result, DbtCloudCheckModelResultOperator)
        self.assertEqual(result.dbt_cloud_conn_id, dbt_job_task.dbt_cloud_conn_id)
        self.assertEqual(result.ensure_models_ran, True)
        self.assertEqual(result.trigger_rule, 'all_done')
        self.assertEqual(result.retries, 0)
        self.assertIn(dbt_job_task, result.get_flat_relatives(upstream=True))
        self.assertIn(task1, result.get_flat_relatives(upstream=False))
        self.assertIn(task2, result.get_flat_relatives(upstream=False))
        

    def test_generate_dbt_model_dependency_with_task_group(self):
        # Create a minimal DAG for testing
        dag = DAG(dag_id='test_dag', start_date=datetime(2023, 1, 1), catchup=False)
        with dag:
            dbt_job_task = DbtCloudRunAndWatchJobOperator(task_id='dbt_job_task', dbt_cloud_conn_id='dbt_default', job_name='test job')

            with TaskGroup(group_id='task_group_name') as downstream_tasks:
                task1 = DummyOperator(task_id='task1')
                task2 = DummyOperator(task_id='task2')

            generate_dbt_model_dependency(dbt_job_task, downstream_tasks, ['model1', 'model2'], ensure_models_ran=False)

        self.assertEqual(len(dag.tasks), 4)
        # find the generated task
        for task in dag.tasks:
            if isinstance(task, DbtCloudCheckModelResultOperator):
                result = task

        # Verify the generated dependencies
        self.assertEqual(result.task_id, 'check_dbt_model_results__dbt_job_task__model1__model2')
        self.assertEqual(result.dbt_cloud_conn_id, dbt_job_task.dbt_cloud_conn_id)
        self.assertEqual(result.ensure_models_ran, False)
        self.assertEqual(result.trigger_rule, 'all_done')
        self.assertEqual(result.retries, 0)
        self.assertIn(dbt_job_task, result.get_flat_relatives(upstream=True))
        self.assertIn(task1, result.get_flat_relatives(upstream=False))
        self.assertIn(task2, result.get_flat_relatives(upstream=False))


if __name__ == '__main__':
    unittest.main()
