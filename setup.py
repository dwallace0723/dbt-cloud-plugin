
from distutils.core import setup

setup(
    name='dbt_cloud_plugin',
    version='0.1',
    packages=[
        'dbt_cloud_plugin',
        'dbt_cloud_plugin.dbt_cloud',
        'dbt_cloud_plugin.hooks',
        'dbt_cloud_plugin.operators',
        'dbt_cloud_plugin.sensors'
    ],
    install_requires=[
        'apache-airflow'
    ]
    
)
