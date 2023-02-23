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

queue_name = "huawei_test"
project_id = "ea31ff23328a4d6bbcca820076f7c606"
    
with DAG(
    "dli", description="Huawei Cloud DLI", start_date=datetime(2022, 10, 29), catchup=False, render_template_as_native_obj=True
) as dag:
    
    # [START howto_operator_dli_list_queue]
    list_queue = DLIListQueuesOperator(
        task_id='dli_list_queue_task',
        project_id=project_id,
        queue_type="all",
        tags="key1=value1",
        return_billing_info=True,
        return_permission_info=True
    )
    # [END howto_operator_dli_list_queue]
    
    # [START howto_operator_dli_create_queue]
    create_queue = DLICreateQueueOperator(
        task_id="dli_create_queue_task",
        project_id=project_id,
        queue_name=queue_name,
        cu_count=16,
        # charging_mode=1,
        # description="Description of Queue",
        # enterprise_project_id="0",
        # feature="basic",
        # platform="x86_64",
        # queue_type="general",
        resource_mode=1,
        # elastic_resource_pool_name="",
        # list_labels_body=["multi_az=2"],
        # list_tags_body=[{"key": "key1", "value": "value1"}],
    )
    # [END howto_operator_dli_create_queue]
    
    # [START howto_operator_dli_update_queue_cidr]
    update_cidr = DLIUpdateQueueCidrOperator(
        task_id="dli_update_cidr", project_id=project_id, queue_name=queue_name, cidr_in_vpc="10.0.0.0/8"
    )
    # [END howto_operator_dli_update_queue_cidr]
    
    # [START howto_operator_dli_upload_files]
    upload_files = DLIUploadFilesOperator(
        task_id="dli_upload_file_task",
        project_id=project_id,
        group="airflow",
        paths=[
            "https://airflow-project.obs.ap-southeast-3.myhuaweicloud.com:443/test.jar"
        ],
    )
    # [END howto_operator_dli_upload_files]
    
    # [START howto_operator_dli_create_batch_job]
    create_batch_job = DLISparkCreateBatchJobOperator(
        task_id="dli_create_batch_job_task",
        project_id=project_id,
        queue_name=queue_name,
        file="testname/test.jar",
        class_name="com.huawei.dli.demo.SparkDemoObs",
        # auto_recovery=False,
        # catalog_name='dli', #static value
        # cluster_name='test_cluster', # Choose queue or cluster
        # driver_cores=8,
        # driver_memory="2G",
        # executor_cores=6,
        # executor_memory="2G",
        # feature="basic", # basic, ai or custom
        # image="image/image", #Should be null in basic/ai
        # max_retry_times=20,
        # name="name",
        # num_executors=2,
        # obs_bucket="airflow-project",
        # sc_type="A",
        # spark_version="2.3.2",
        # list_args_body=["--h","--v"]
        # list_conf_body=[],
        # list_files_body=[],
        # list_groups_body=[],
        # list_jars_body=[],
        # list_modules_body=[],
        # list_python_files_body=[],
        # list_resources_body=[],
    )
    # [END howto_operator_dli_create_batch_job]
    
    # [START howto_sensor_dli_show_batch_state]
    batch_state_sensor = DLISparkShowBatchStateSensor(
        task_id="dli_batch_state_task", project_id=project_id, job_id="{{ti.xcom_pull(task_ids='dli_create_batch_job_task')['id']}}"
    )
    # [END howto_sensor_dli_show_batch_state]
    
    # [START howto_operator_dli_delete_queue]
    delete_queue = DLIDeleteQueueOperator(
        task_id="dli_delete_queue_task", project_id=project_id, queue_name=queue_name
    )
    # [END howto_operator_dli_delete_queue]
    
    # [START howto_operator_dli_run_job]
    run_job = DLIRunSqlJobOperator(
        task_id="dli_run_job_task",
        project_id=project_id,
        database_name="tpch",
        sql_query="select * from region",
        queue_name=queue_name,
        #list_conf_body=["dli.sql.shuffle.partitions = 200"],
        #list_tags_body=[{"key": "key", "value" : "val"}]
    )
    # [END howto_operator_dli_run_job]
    
    # [START howto_sensor_dli_show_job_status]
    job_status_sensor = DLISqlShowJobStatusSensor(
        task_id="dli_show_job_status", project_id=project_id, job_id="{{ti.xcom_pull(task_ids='dli_run_job_task')['job_id']}}"
    )
    # [END howto_sensor_dli_show_job_status]
    # [START howto_operator_dli_get_job_result]
    sql_job_result = DLIGetSqlJobResultOperator(
        task_id="dli_get_job_result", project_id=project_id, queue_name=queue_name, job_id="{{ti.xcom_pull(task_ids='dli_run_job_task')['job_id']}}"
    )
    # [END howto_operator_dli_get_job_result]
    create_queue >> update_cidr >> upload_files >> create_batch_job >> batch_state_sensor >> delete_queue

    run_job >> job_status_sensor >> sql_job_result