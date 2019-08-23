# dbt-cloud-plugin
DBT Cloud Plugin for Airflow

## Configuration

Copy the `dbt_cloud_plugin` directory in Airflow's `plugin` directory.

Create a new connection with the following dictionary as the `Extra` parameter. Leave connection type blank.
```
{
  "dbt_cloud_api_token": "123abcdefg456",
  "dbt_cloud_account_id": 12345678
}
```

In order to obtain your API token, log into your [dbt Cloud Account](https://cloud.getdbt.com), click on your Avatar in the top right corner, then `My Account` and finally on `API Access` in the left bar. 

Note: API Access is not available on the _Free_ plan. 


In order to test if the connection is set up correctly, log onto the Airflow shell and run

`airflow test --dry_run dbt_cloud_dag run_dbt_cloud_job 2019-01-01`



----
MIT License