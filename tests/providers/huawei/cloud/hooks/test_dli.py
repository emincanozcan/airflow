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


from airflow.providers.huawei.cloud.hooks.dli import DLIHook
from tests.providers.huawei.cloud.utils.hw_mock import mock_huawei_cloud_default

DLI_STRING = "airflow.providers.huawei.cloud.hooks.dli.{}"
MOCK_DLI_CONN_ID = "mock_dli_default"


class TestDliHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            DLI_STRING.format("DLIHook.__init__"),
            new=mock_huawei_cloud_default,
        ):
            self.hook = DLIHook(huaweicloud_conn_id=MOCK_DLI_CONN_ID)

    def test_get_default_region(self):
        assert self.hook.get_region() == "ap-southeast-3"

    def test_get_dli_client(self):
        client = self.hook._get_dli_client("project_id")
        assert client.get_credentials().ak == "AK"
        assert client.get_credentials().sk == "SK"
        assert client.get_credentials().project_id == "project_id"

    @mock.patch(DLI_STRING.format("DliSdk.DliClient.create_queue"))
    def test_create_queue(self, create_queue):
        project_id = "example-id"
        queue_name = "test_queue"
        charging_mode = "charging_mode_test"
        cu_count = "test"
        description = "description_test"
        elastic_resource_pool_name = "elastic_resource_pool_name_test"
        feature = "feature_test"
        listTagsbody = [{"key": "key_test", "value": "value_test"}]
        listLabelsbody = ["test_label=2"]
        enterprise_project_id = "enterprise_project_id_test"
        platform = "platform_test"
        queue_type = "queue_type_test"
        resource_mode = "resource_mode_test"
        self.hook.create_queue(
            project_id,
            queue_name,
            platform,
            enterprise_project_id,
            elastic_resource_pool_name,
            feature,
            resource_mode,
            charging_mode,
            description,
            queue_type,
            listTagsbody,
            listLabelsbody,
            cu_count,
        )
        request = self.hook.create_queue_request(
            queue_name,
            platform,
            enterprise_project_id,
            elastic_resource_pool_name,
            feature,
            resource_mode,
            charging_mode,
            description,
            queue_type,
            listTagsbody,
            listLabelsbody,
            cu_count,
        )
        create_queue.assert_called_once_with(request)

    @mock.patch(DLI_STRING.format("DliSdk.DliClient.update_queue_cidr"))
    def test_update_queue_cidr(self, update_queue_cidr):
        project_id = "example-id"
        queue_name = "test_queue"
        cidr_in_vpc = "10.0.0.0/8"
        self.hook.update_queue_cidr(project_id, queue_name, cidr_in_vpc)
        request = self.hook.update_queue_cidr_request(queue_name, cidr_in_vpc)
        update_queue_cidr.assert_called_once_with(request)

    @mock.patch(DLI_STRING.format("DliSdk.DliClient.delete_queue"))
    def test_delete_queue(self, delete_queue):
        project_id = "example-id"
        queue_name = "queue_name"
        self.hook.delete_queue(project_id=project_id, queue_name=queue_name)
        request = self.hook.delete_queue_request(queue_name)
        delete_queue.assert_called_once_with(request)

    @mock.patch(DLI_STRING.format("DliSdk.DliClient.list_queues"))
    def test_list_queues(self, list_queues):
        project_id = "example-id"
        queue_type = "queue_type"
        tags = "tags"
        return_billing_info = False
        return_permission_info = False
        self.hook.list_queues(
            project_id=project_id,
            queue_type=queue_type,
            return_billing_info=return_billing_info,
            tags=tags,
            return_permission_info=return_permission_info,
        )
        request = self.hook.list_queues_request(
            queue_type=queue_type,
            return_billing_info=return_billing_info,
            tags=tags,
            return_permission_info=return_permission_info,
        )
        list_queues.assert_called_once_with(request)

    @mock.patch(DLI_STRING.format("DliSdk.DliClient.show_job_status"))
    def test_show_job_status(self, show_job_status):
        project_id = "project-id"
        job_id = "job-id"
        self.hook.show_job_status(project_id=project_id, job_id=job_id)
        request = self.hook.show_job_status_request(job_id)
        show_job_status.assert_called_once_with(request)

    @mock.patch(DLI_STRING.format("DliSdk.DliClient.show_batch_state"))
    def test_show_batch_state(self, show_batch_state):
        project_id = "project-id"
        job_id = "job-id"
        self.hook.show_batch_state(project_id=project_id, job_id=job_id)
        request = self.hook.show_batch_state_request(job_id)
        show_batch_state.assert_called_once_with(request)

    @mock.patch(DLI_STRING.format("DliSdk.DliClient.run_job"))
    def test_run_job(self, run_job):
        project_id = "project-id"
        queue_name = "queue_name"
        currentdb = "current-db"
        sql = "sql-query"
        tags = "tags"
        conf = "list"
        self.hook.run_job(
            project_id=project_id,
            queue_name=queue_name,
            database_name=currentdb,
            list_conf_body=conf,
            list_tags_body=tags,
            sql_query=sql,
        )
        request = self.hook.run_job_request(
            queue_name=queue_name,
            database_name=currentdb,
            list_conf_body=conf,
            list_tags_body=tags,
            sql_query=sql,
        )
        run_job.assert_called_once_with(request)

    @mock.patch(DLI_STRING.format("DliSdk.DliClient.upload_files"))
    def test_upload_files(self, upload_files):
        project_id = "example-id"
        file_path = "obs://bucketname/test"
        group = "test-group"
        self.hook.upload_files(project_id=project_id, file_path=file_path, group=group)
        request = self.hook.upload_files_request(file_path=file_path, group=group)
        upload_files.assert_called_once_with(request)

    @mock.patch(DLI_STRING.format("DliSdk.DliClient.create_batch_job"))
    def test_create_batch_job(self, create_batch_job):
        project_id = "example-id"
        queue_name = "test_queue"
        file_path = "obs://bucketname/test"
        class_name = "com.packagename.ClassName"
        spark_version = "2.3.2"
        feature = "basic"
        name = "test"
        listGroupsbody = [
            {
                "name": "groupTestJar",
                "resources": [
                    {"name": "testJar.jar", "type": "jar"},
                    {"name": "testJar1.jar", "type": "jar"},
                ],
            },
            {"name": "batchTest", "resources": [{"name": "luxor.jar", "type": "jar"}]},
        ]
        listResourcesbody = [
            {"name": "groupTest/testJar.jar", "type": "jar"},
            {"name": "kafka-clients-0.10.0.0.jar", "type": "jar"},
        ]
        listFilesbody = ["count.txt"]
        listJarsbody = ["demo-1.0.0.jar"]
        sc_type = "A"
        obs_bucket = "airflow"
        catalog_name = "catalog-name"
        listConfbody = []
        listModulesbody = []
        listPythonFilesbody = []
        listArgsbody = []
        max_retry_times = 20
        image = "image"
        auto_recovery = False
        cluster_name = "cluster"
        num_executors = 6
        executor_cores = 6
        executor_memory = "2G"
        driver_cores = 6
        driver_memory = "2G"
        self.hook.create_batch_job(
            project_id=project_id,
            queue_name=queue_name,
            file_path=file_path,
            class_name=class_name,
            obs_bucket=obs_bucket,
            catalog_name=catalog_name,
            image=image,
            max_retry_times=max_retry_times,
            auto_recovery=auto_recovery,
            spark_version=spark_version,
            feature=feature,
            num_executors=num_executors,
            executor_cores=executor_cores,
            executor_memory=executor_memory,
            driver_cores=driver_cores,
            driver_memory=driver_memory,
            name=name,
            list_conf_body=listConfbody,
            list_groups_body=listGroupsbody,
            list_resources_body=listResourcesbody,
            list_modules_body=listModulesbody,
            list_files_body=listFilesbody,
            list_python_files_body=listPythonFilesbody,
            list_jars_body=listJarsbody,
            sc_type=sc_type,
            list_args_body=listArgsbody,
            cluster_name=cluster_name,
        )
        request = self.hook.create_batch_job_request(
            queue_name=queue_name,
            file_path=file_path,
            class_name=class_name,
            obs_bucket=obs_bucket,
            catalog_name=catalog_name,
            image=image,
            max_retry_times=max_retry_times,
            auto_recovery=auto_recovery,
            spark_version=spark_version,
            feature=feature,
            num_executors=num_executors,
            executor_cores=executor_cores,
            executor_memory=executor_memory,
            driver_cores=driver_cores,
            driver_memory=driver_memory,
            name=name,
            list_conf_body=listConfbody,
            list_groups_body=listGroupsbody,
            list_resources_body=listResourcesbody,
            list_modules_body=listModulesbody,
            list_files_body=listFilesbody,
            list_python_files_body=listPythonFilesbody,
            list_jars_body=listJarsbody,
            sc_type=sc_type,
            list_args_body=listArgsbody,
            cluster_name=cluster_name,
        )
        create_batch_job.assert_called_once_with(request)

    @mock.patch(DLI_STRING.format("DliSdk.DliClient.call_api"))
    def test_create_queue_calls_api(self, call_api):
        project_id = "example-id"
        queue_name = "test_queue"
        charging_mode = {"key": "value"}
        cu_count = "test"
        description = "description_test"
        elastic_resource_pool_name = "elastic_resource_pool_name_test"
        feature = "feature_test"
        listTagsbody = [{"key": "key_test", "value": "value_test"}]
        listLabelsbody = ["test_label=2"]
        enterprise_project_id = "enterprise_project_id_test"
        platform = "platform_test"
        queue_type = "queue_type_test"
        resource_mode = "resource_mode_test"
        self.hook.create_queue(
            project_id=project_id,
            charging_mode=charging_mode,
            cu_count=cu_count,
            description=description,
            elastic_resource_pool_name=elastic_resource_pool_name,
            enterprise_project_id=enterprise_project_id,
            feature=feature,
            list_labels_body=listLabelsbody,
            list_tags_body=listTagsbody,
            platform=platform,
            queue_name=queue_name,
            queue_type=queue_type,
            resource_mode=resource_mode,
        )
        request_test = self.hook.create_queue_request(
            charging_mode=charging_mode,
            cu_count=cu_count,
            description=description,
            elastic_resource_pool_name=elastic_resource_pool_name,
            enterprise_project_id=enterprise_project_id,
            feature=feature,
            list_labels_body=listLabelsbody,
            list_tags_body=listTagsbody,
            platform=platform,
            queue_name=queue_name,
            queue_type=queue_type,
            resource_mode=resource_mode,
        )
        call_api.assert_called_once_with(
            resource_path="/v1.0/{project_id}/queues",
            method="POST",
            path_params={},
            query_params=[],
            header_params={"Content-Type": "application/json"},
            body=request_test.body,
            post_params={},
            cname=None,
            response_type="CreateQueueResponse",
            response_headers=[],
            auth_settings=[],
            collection_formats={},
            request_type="CreateQueueRequest",
        )
