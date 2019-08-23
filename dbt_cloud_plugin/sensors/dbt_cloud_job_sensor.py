from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.sensors.base_sensor_operator import BaseSensorOperator

from dbt_cloud_plugin.hooks.dbt_cloud_hook import DbtCloudHook


class DbtCloudRunSensor(BaseSensorOperator):
    """
    Asks for the state of a dbt Cloud job until it reaches a terminal state.
    If it fails the sensor errors, failing the task.
    :param dbt_cloud_conn_id: dbt Cloud connection ID.
    :type dbt_cloud_conn_id: string
    :param run_id: dbt cloud run ID.
    :type run_ID: int
    """
    template_fields = ['run_id']
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 dbt_cloud_conn_id=None,
                 run_id=None,
                 *args, **kwargs):
        super(DbtCloudRunSensor, self).__init__(*args, **kwargs)

        if dbt_cloud_conn_id is None:
            raise AirflowException('No valid dbt Cloud connection ID was supplied.')

        if run_id is None:
            raise AirflowException('No dbt Cloud Run ID was supplied.')

        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.run_id = run_id

    def poke(self, context):
        self.log.info(f'Sensor checking state of dbt Cloud Run ID: {self.run_id}')
        dbt_cloud_hook = DbtCloudHook(dbt_cloud_conn_id=self.dbt_cloud_conn_id)
        run_status = dbt_cloud_hook.get_run_status(run_id=self.run_id)
        self.log.info(f'State of Run ID {self.run_id}: {run_status}')

        TERMINAL_RUN_STATES = ['Success', 'Cancelled']
        FAILED_RUN_STATES = ['Error']

        if run_status in FAILED_RUN_STATES:
            raise AirflowException(
                f'dbt Cloud Run ID {self.run_id} failed.'
            )
        if run_status in TERMINAL_RUN_STATES:
            return True
        else:
            return False
