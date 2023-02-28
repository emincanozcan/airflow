from datetime import datetime
from airflow import DAG

from airflow.providers.huawei.cloud.operators.dataarts import (
    DataArtsDLFStartJobOperator
)

from airflow.providers.huawei.cloud.sensors.dataarts import DataArtsDLFShowJobStatusSensor

project_id = "ea31ff23328a4d6bbcca820076f7c606"
job_name = "job_name"
workspace = "workspace-id"
body = {"jobParams": [{"name": "param1", "value": "value1"}]}


with DAG(
    "dlf", description="Huawei Cloud DLF", start_date=datetime(2022, 10, 29), catchup=False, render_template_as_native_obj=True
) as dag:
    
    # [START howto_operator_dlf_start_job]
    start_job = DataArtsDLFStartJobOperator(
        task_id="dlf_start_job",
        workspace=workspace,
        body=body,
        job_name=job_name
    )
    # [END howto_operator_dlf_start_job]

    # [START howto_sensor_dlf_show_job_status]
    job_status_sensor = DataArtsDLFShowJobStatusSensor(
        task_id="dlf_show_job_status",  
        job_name=job_name, 
        workspace=workspace
    )
    # [END howto_sensor_dlf_show_job_status]
    
