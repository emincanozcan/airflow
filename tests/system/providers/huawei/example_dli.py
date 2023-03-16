from datetime import datetime
from airflow import DAG

from airflow.providers.huawei.cloud.operators.dli import (
    DLICreateQueueOperator,
    DLIDeleteQueueOperator,
    DLIListQueuesOperator,
    DLISparkCreateBatchJobOperator,
    DLIUploadFilesOperator,
    DLIRunSqlJobOperator,
    DLIUpdateQueueCidrOperator,
    DLIGetSqlJobResultOperator
)

from airflow.providers.huawei.cloud.sensors.dli import DLISparkShowBatchStateSensor, DLISqlShowJobStatusSensor

queue_name_spark = "spark_queue"
queue_name_sql= "sql_queue"
    
with DAG(
    "dli", description="Huawei Cloud DLI", start_date=datetime(2022, 10, 29), catchup=False, render_template_as_native_obj=True
) as dag:
    
    # [START howto_operator_dli_list_queue]
    list_queue = DLIListQueuesOperator(
        task_id='dli_list_queue_task',
        queue_type="all",
        return_billing_info=True,
        return_permission_info=True
    )
    # [END howto_operator_dli_list_queue]
    
    # [START howto_operator_dli_create_queue]
    # Queues used to run SQL jobs.
    create_sql_queue = DLICreateQueueOperator(
        task_id="dli_create_sql_queue_task",
        queue_name=queue_name_sql,
        cu_count=16,
    )
    
    # Queues used to run Flink and Spark Jar jobs.
    create_spark_queue = DLICreateQueueOperator(
        task_id="dli_create_spark_queue_task",
        queue_name=queue_name_spark,
        cu_count=16,
        queue_type="general",
        resource_mode=1
    )
    # [END howto_operator_dli_create_queue]
    
    # [START howto_operator_dli_update_queue_cidr]
    update_cidr = DLIUpdateQueueCidrOperator(
        task_id="dli_update_cidr",  
        queue_name=queue_name_spark, 
        cidr_in_vpc="10.0.0.0/8"
    )
    # [END howto_operator_dli_update_queue_cidr]
    
    # [START howto_operator_dli_upload_files]
    upload_files = DLIUploadFilesOperator(
        task_id="dli_upload_file_task",
        group="airflow",
        paths=[
            "https://airflow-project.obs.ap-southeast-3.myhuaweicloud.com:443/myjarfile.jar"
        ],
    )
    # [END howto_operator_dli_upload_files]
    
    # [START howto_operator_dli_create_batch_job]
    create_batch_job = DLISparkCreateBatchJobOperator(
        task_id="dli_create_batch_job_task",
        queue_name=queue_name_spark,
        file="airflow-project/myjarfile.jar",
        class_name="com.huawei.dli.demo.SparkDemoObs",
    )
    # [END howto_operator_dli_create_batch_job]
    
    # [START howto_sensor_dli_show_batch_state]
    batch_state_sensor = DLISparkShowBatchStateSensor(
        task_id="dli_batch_state_task",  
        job_id="{{ti.xcom_pull(task_ids='dli_create_batch_job_task')['id']}}"
    )
    # [END howto_sensor_dli_show_batch_state]
    
    # [START howto_operator_dli_delete_queue]
    delete_spark_queue = DLIDeleteQueueOperator(
        task_id="dli_delete_queue_task",  
        queue_name=queue_name_spark
    )
    # [END howto_operator_dli_delete_queue]
    
    # [START howto_operator_dli_run_job]
    run_job = DLIRunSqlJobOperator(
        task_id="dli_run_job_task",
        database_name="DBNAME",
        sql_query="select * from table_name",
        queue_name=queue_name_sql
    )
    # [END howto_operator_dli_run_job]
    
    # [START howto_sensor_dli_show_job_status]
    job_status_sensor = DLISqlShowJobStatusSensor(
        task_id="dli_show_job_status",  
        job_id="{{ti.xcom_pull(task_ids='dli_run_job_task')['job_id']}}"
    )
    # [END howto_sensor_dli_show_job_status]
    # [START howto_operator_dli_get_job_result]
    sql_job_result = DLIGetSqlJobResultOperator(
        task_id="dli_get_job_result",  
        queue_name=queue_name_sql, 
        job_id="{{ti.xcom_pull(task_ids='dli_run_job_task')['job_id']}}"
    )
    # [END howto_operator_dli_get_job_result]
    
    delete_sql_queue = DLIDeleteQueueOperator(
        task_id="dli_delete_sql_queue",
        queue_name=queue_name_sql
    )

    create_spark_queue >> update_cidr >> upload_files >> create_batch_job >> batch_state_sensor >> delete_spark_queue

    create_sql_queue >> run_job >> job_status_sensor >> sql_job_result >> delete_sql_queue