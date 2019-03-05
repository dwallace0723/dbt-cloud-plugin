from dbt_cloud_plugin.hooks.dbt_cloud_hook import DbtCloudHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

class DbtCloudJobSensor(BaseSensorOperator):
    """
    Asks for the state of a dbt Cloud job until it reaches a terminal state.
    If it fails the sensor errors, failing the task.
    :param dbt_cloud_conn_id: dbt Cloud connection ID.
    :type dbt_cloud_conn_id: string
    :param project_id: dbt Cloud project ID.
    :type project_id: int
    :param job_id: dbt Cloud job ID.
    :type job_ID: int
    """
    template_fields = ['job_id']
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 dbt_cloud_conn_id=None,
                 project_id=None,
                 job_id=None,
                 *args, **kwargs):
        super(DbtCloudJobSensor, self).__init__(*args, **kwargs)

        if dbt_cloud_conn_id is None:
            raise AirflowException('No valid dbt cloud connection ID was supplied.')

        if project_id is None:
            raise AirflowException('No valid project id was supplied.')

        if job_id is None:
            raise AirflowException('No job ID was supplied.')

        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.project_id = project_id
        self.job_id = job_id

    def poke(self, context):
        self.log.info('Sensor checking state of dbt Cloud job ID: %s', self.job_id)
        dbt_cloud_hook = DbtCloudHook(dbt_cloud_conn_id=self.dbt_cloud_conn_id)
        job_status = dbt_cloud_hook.get_job_status(project_id=self.project_id, job_id=self.job_id)
        self.log.info('State of Job ID {}: {}'.format(self.job_id, job_status))

        TERMINAL_JOB_STATES = ['Success', 'Error', 'Cancelled']
        FAILED_JOB_STATES = ['Error']

        if job_status in FAILED_JOB_STATES:
            return AirflowException('dbt Cloud Job ID {} Failed.'.format(self.job_id))
        if job_status in TERMINAL_JOB_STATES:
            return True
        else:
            return False
