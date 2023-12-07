from airflow.utils.task_group import TaskGroup
from airflow.models import BaseOperator
from airflow.operators.python_operator import ShortCircuitOperator

from .operators.dbt_cloud_check_model_result_operator import DbtCloudCheckModelResultOperator
from .operators.dbt_cloud_run_job_operator import DbtCloudRunJobOperator


def generate_dbt_model_dependency(dbt_job_task, downstream_tasks, dependent_models, ensure_models_ran=True, retries=0):
    """
    Create a dependency from one or more tasks on a set of models succeeding
    in a dbt task. This function generates a new DbtCloudCheckModelResultOperator
    task between dbt_job_task and downstream_tasks, checking that dependent_models
    all ran successfully.

    :param dbt_job_task: The dbt Cloud operator which kicked off the run you want to check.
        Both the credentials and the run_id will be pulled from this task.
    :type dbt_job_task: DbtCloudRunJobOperator or DbtCloudRunAndWatchJobOperator
    :param downstream_tasks: The downstream task(s) which depend on the model(s) succeeding.
        Can be either a single task, a single TaskGroup, or a list of tasks.
    :type downstream_tasks: BaseOperator or TaskGroup or list[BaseOperator] or list[TaskGroup]
    :param dependent_models: The name(s) of the model(s) to check. See
        DbtCloudCheckModelResultOperator for more details.
    :type dependent_models: str or list[str]
    :param ensure_models_ran: Whether to require that the dependent_models actually ran in
        the run. If False, it will silently ignore models that didn't run.
    :type ensure_models_ran: bool, default True
    """

    if not isinstance(dbt_job_task, DbtCloudRunJobOperator):
        raise TypeError('dbt_job_task must be of type DbtCloudRunJobOperator or DbtCloudRunAndWatchOperator')
    
    if isinstance(downstream_tasks, list):
        if len(downstream_tasks) == 0:
            raise ValueError('You must pass at least one task in downstream_tasks')
        if not (isinstance(downstream_tasks[0], BaseOperator) or isinstance(downstream_tasks[0], TaskGroup)):
            raise TypeError('The elements of the downstream_tasks list must be of type BaseOperator or TaskGRoup')
    elif not (isinstance(downstream_tasks, TaskGroup) or isinstance(downstream_tasks, BaseOperator)):
        raise TypeError('downstream_tasks must be of one of the following types: BaseOperator, TaskGroup, or a list of one of those two')

    if isinstance(dependent_models, str):
        dependent_models = [dependent_models]
    model_ids = '__'.join(dependent_models)
    task_id = f'check_dbt_model_results__{dbt_job_task.task_id}__{model_ids}'
    task_id = task_id[:255].replace('.', '__')

    with TaskGroup(group_id=task_id) as check_dbt_model_results:
        check_upstream_dbt_job_state = ShortCircuitOperator(
            task_id='check_upstream_dbt_job_state',
            python_callable=lambda **context: context['dag_run'].get_task_instance(dbt_job_task).state in ['success', 'failed'],
            trigger_rule='all_done',
            on_failure_callback=None,
            provide_context=True
        )

        check_dbt_model_successful = DbtCloudCheckModelResultOperator(
            task_id='check_dbt_model_successful',
            dbt_cloud_conn_id=dbt_job_task.dbt_cloud_conn_id,
            dbt_cloud_run_id=f'{{{{ ti.xcom_pull(task_ids="{dbt_job_task.task_id}", key="dbt_cloud_run_id") }}}}',
            model_names=dependent_models,
            ensure_models_ran=ensure_models_ran,
            retries=retries
        )

        check_upstream_dbt_job_state >> check_dbt_model_successful

    return dbt_job_task >> check_dbt_model_results >> downstream_tasks