from ..operators.dbt_cloud_check_model_result_operator import DbtCloudCheckModelResultOperator


def generate_dbt_model_dependency(dbt_job_task, downstream_tasks, dependent_models):
    check_dbt_model_results = DbtCloudCheckModelResultOperator(
        task_id=f'check_dbt_model_results__{dbt_task.task_id}__{downstream_task.task_id}',
        dbt_cloud_conn_id=dbt_job_task.dbt_cloud_conn_id,
        dbt_cloud_run_id=f'{{{{ ti.xcom_pull(task_ids="{dbt_task.task_id}", key="dbt_cloud_run_id") }}}}',
        model_names=dependent_models,
        trigger_rule='all_done',
        retries=0
    )

    return dbt_job_task >> check_dbt_model_results >> downstream_tasks
