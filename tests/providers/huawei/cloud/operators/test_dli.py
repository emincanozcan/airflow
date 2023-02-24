#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations
import json

import unittest
from unittest import mock

from airflow.providers.huawei.cloud.operators.dli import (
    DLISparkCreateBatchJobOperator,
    DLICreateQueueOperator,
    DLIDeleteQueueOperator,
    DLIListQueuesOperator,
    DLIRunSqlJobOperator,
    DLIUpdateQueueCidrOperator,
    DLIUploadFilesOperator,
    DLIGetSqlJobResultOperator
)

MOCK_TASK_ID = "test-dli-operator"
MOCK_REGION = "mock_region"
MOCK_DLI_CONN_ID = "mock_dli_conn_default"
MOCK_PROJECT_ID = None
MOCK_JOB_ID = "mock_job_id"
MOCK_AUTO_RECOVERY = False
MOCK_CATALOG_NAME = "catalog_name"
MOCK_CLASS_NAME = "com.packagename.ClassName"
MOCK_CLUSTER_NAME = "cluster_name"
MOCK_DRIVER_CORES = 4
MOCK_DRIVER_MEMORY = "2G"
MOCK_EXECUTER_CORES = 4
MOCK_EXECUTER_MEMORY = "executer_memory"
MOCK_FEATURE = "feature"
MOCK_FILE_PATHS = ["file_path"]
MOCK_FILE_PATH = "file_path"
MOCK_IMAGE = "image"
MOCK_NAME = "name"
MOCK_OBS_BUCKET = "bucket"
MOCK_QUEUE_NAME = "queue_name"
MOCK_SC_TYPE = "sc_type"
MOCK_SPARK_VERSION = "1.0.0"
MOCK_NUM_EXECUTORS = 10
MOCK_MAX_RETRY_TIMES = 20
MOCK_LIST_ARGS_BODY = []
MOCK_LIST_CONF_BODY = []

MOCK_CHARGING_MODE = 1
MOCK_CU_COUNT = 16
MOCK_DESCRIPTION = "description"
MOCK_ELASTIC_RESOURCE_POOL_NAME = "pool_1"
MOCK_ENTERPRISE_PROJECT_ID = "0"
MOCK_FEATURE = "basic"
MOCK_LISTLABELSBODY = ["multi_az=2"]
MOCK_LIST_TAGS_BODY = [{"key":"test_key", "value": "test_value"}]
MOCK_PLATFORM = "x86_64"
MOCK_QUEUE_NAME = "test_queue"
MOCK_QUEUE_TYPE = "sql"
MOCK_RESOURCE_MODE = 1

MOCK_BILLING_INFO = False
MOCK_PERMISSION_INFO = False
MOCK_TAGS = "mock_tags"

MOCK_DATABASE_NAME = "mock_db"
MOCK_SQL_QUERY = "select * from mock"
MOCK_CIDR_IN_VPC = "10.0.0.1/8"
MOCK_GROUP = "mock_group"


class TestDLISparkCreateBatchJobOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.dli.DLIHook")
    def test_execute(self, mock_hook):
        operator = DLISparkCreateBatchJobOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_DLI_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            auto_recovery=MOCK_AUTO_RECOVERY,
            catalog_name=MOCK_CATALOG_NAME,
            class_name=MOCK_CLASS_NAME,
            cluster_name=MOCK_CLUSTER_NAME,
            driver_cores=MOCK_DRIVER_CORES,
            driver_memory=MOCK_DRIVER_MEMORY,
            executor_cores=MOCK_EXECUTER_CORES,
            executor_memory=MOCK_EXECUTER_MEMORY,
            feature=MOCK_FEATURE,
            file=MOCK_FILE_PATH,
            image=MOCK_IMAGE,
            name=MOCK_NAME,
            obs_bucket=MOCK_OBS_BUCKET,
            queue_name=MOCK_QUEUE_NAME,
            sc_type=MOCK_SC_TYPE,
            spark_version=MOCK_SPARK_VERSION,
            num_executors=MOCK_NUM_EXECUTORS,
            max_retry_times=MOCK_MAX_RETRY_TIMES,
            list_args_body=MOCK_LIST_ARGS_BODY,
            list_conf_body=MOCK_LIST_CONF_BODY,
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DLI_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID)
        mock_hook.return_value.create_batch_job.assert_called_once_with(
            auto_recovery=MOCK_AUTO_RECOVERY,
            catalog_name=MOCK_CATALOG_NAME,
            class_name=MOCK_CLASS_NAME,
            cluster_name=MOCK_CLUSTER_NAME,
            driver_cores=MOCK_DRIVER_CORES,
            driver_memory=MOCK_DRIVER_MEMORY,
            executor_cores=MOCK_EXECUTER_CORES,
            executor_memory=MOCK_EXECUTER_MEMORY,
            feature=MOCK_FEATURE,
            file=MOCK_FILE_PATH,
            image=MOCK_IMAGE,
            name=MOCK_NAME,
            obs_bucket=MOCK_OBS_BUCKET,
            queue_name=MOCK_QUEUE_NAME,
            sc_type=MOCK_SC_TYPE,
            spark_version=MOCK_SPARK_VERSION,
            num_executors=MOCK_NUM_EXECUTORS,
            max_retry_times=MOCK_MAX_RETRY_TIMES,
            list_args_body=MOCK_LIST_ARGS_BODY,
            list_conf_body=MOCK_LIST_CONF_BODY,
            list_files_body=None,
            list_groups_body=None,
            list_jars_body=None,
            list_modules_body=None,
            list_python_files_body=None,
            list_resources_body=None
        )

class TestDLICreateQueueOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.dli.DLIHook")
    def test_execute(self, mock_hook):
        operator = DLICreateQueueOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_DLI_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            charging_mode=MOCK_CHARGING_MODE,
            cu_count=MOCK_CU_COUNT,
            description=MOCK_DESCRIPTION,
            elastic_resource_pool_name=MOCK_ELASTIC_RESOURCE_POOL_NAME,
            enterprise_project_id=MOCK_ENTERPRISE_PROJECT_ID,
            feature=MOCK_FEATURE,
            list_labels_body=MOCK_LISTLABELSBODY,
            list_tags_body=MOCK_LIST_TAGS_BODY,
            platform=MOCK_PLATFORM,
            queue_name=MOCK_QUEUE_NAME,
            queue_type=MOCK_QUEUE_TYPE,
            resource_mode=MOCK_RESOURCE_MODE
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DLI_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID)
        mock_hook.return_value.create_queue.assert_called_once_with(
            charging_mode=MOCK_CHARGING_MODE,
            cu_count=MOCK_CU_COUNT,
            description=MOCK_DESCRIPTION,
            elastic_resource_pool_name=MOCK_ELASTIC_RESOURCE_POOL_NAME,
            enterprise_project_id=MOCK_ENTERPRISE_PROJECT_ID,
            feature=MOCK_FEATURE,
            list_labels_body=MOCK_LISTLABELSBODY,
            list_tags_body=MOCK_LIST_TAGS_BODY,
            platform=MOCK_PLATFORM,
            queue_name=MOCK_QUEUE_NAME,
            queue_type=MOCK_QUEUE_TYPE,
            resource_mode=MOCK_RESOURCE_MODE
        )

class TestDLIDeleteQueueOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.dli.DLIHook")
    def test_execute(self, mock_hook):
        operator = DLIDeleteQueueOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_DLI_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            queue_name=MOCK_QUEUE_NAME
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DLI_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID)
        mock_hook.return_value.delete_queue.assert_called_once_with(
            queue_name=MOCK_QUEUE_NAME)

class TestDLIGetSqlJobResultOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.dli.DLIHook")
    def test_execute(self, mock_hook):
        operator = DLIGetSqlJobResultOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_DLI_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            queue_name=MOCK_QUEUE_NAME,
            job_id=MOCK_JOB_ID
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DLI_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID)
        mock_hook.return_value.get_job_result.assert_called_once_with(
            queue_name=MOCK_QUEUE_NAME,
            job_id=MOCK_JOB_ID)

class TestDLIListQueueOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.dli.DLIHook")
    def test_execute(self, mock_hook):
        operator = DLIListQueuesOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_DLI_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            queue_type=MOCK_QUEUE_TYPE,
            return_billing_info=MOCK_BILLING_INFO,
            return_permission_info=MOCK_PERMISSION_INFO,
            tags=MOCK_TAGS
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DLI_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID)
        mock_hook.return_value.list_queues.assert_called_once_with(
            queue_type=MOCK_QUEUE_TYPE,
            return_billing_info=MOCK_BILLING_INFO,
            return_permission_info=MOCK_PERMISSION_INFO,
            tags=MOCK_TAGS)

class TestDLIRunSqlJobOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.dli.DLIHook")
    def test_execute(self, mock_hook):
        operator = DLIRunSqlJobOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_DLI_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            database_name=MOCK_DATABASE_NAME,
            list_conf_body=MOCK_LIST_CONF_BODY,
            list_tags_body=MOCK_LIST_TAGS_BODY,
            queue_name=MOCK_QUEUE_NAME,
            sql_query=MOCK_SQL_QUERY
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DLI_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID)
        mock_hook.return_value.run_job.assert_called_once_with(
            database_name=MOCK_DATABASE_NAME,
            list_conf_body=MOCK_LIST_CONF_BODY,
            list_tags_body=MOCK_LIST_TAGS_BODY,
            queue_name=MOCK_QUEUE_NAME,
            sql_query=MOCK_SQL_QUERY)

class TestDLIUpdateQueueCidrOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.dli.DLIHook")
    def test_execute(self, mock_hook):
        operator = DLIUpdateQueueCidrOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_DLI_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            cidr_in_vpc=MOCK_CIDR_IN_VPC,
            queue_name=MOCK_QUEUE_NAME
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DLI_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID)
        mock_hook.return_value.update_queue_cidr.assert_called_once_with(
            cidr_in_vpc=MOCK_CIDR_IN_VPC,
            queue_name=MOCK_QUEUE_NAME)

class TestDLIUploadFilesOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.dli.DLIHook")
    def test_execute(self, mock_hook):
        operator = DLIUploadFilesOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_DLI_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            paths=MOCK_FILE_PATHS,
            group=MOCK_GROUP
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DLI_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID)
        mock_hook.return_value.upload_files.assert_called_once_with(
            paths=MOCK_FILE_PATHS,
            group=MOCK_GROUP)