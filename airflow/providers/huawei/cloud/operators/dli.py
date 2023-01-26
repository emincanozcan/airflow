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
"""This module contains Huawei Cloud SMN operators."""
from __future__ import annotations

from typing import TYPE_CHECKING
import json

from airflow.models import BaseOperator
from airflow.providers.huawei.cloud.hooks.dli import DLIHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DLICreateQueueOperator(BaseOperator):
    def __init__(
        self,
        project_id: str,
        queue_name: str,
        cu_count: int,
        platform: str | None = None,
        enterprise_project_id: str | None = None,
        feature: str | None = None,  # basic or ai(Only the SQL x86_64 dedicated queue supports this option)
        resource_mode: int | None = None,  # 0 Shared or 1 Exclusive
        charging_mode: int | None = None,  # Set only 1
        description: str | None = None,
        queue_type: str = "general",  # sql or general
        list_tags_body: list[object] | None = None,
        list_labels_body: list[object] | None = None,  # TODO: Ask to HQ for detail
        elastic_resource_pool_name: str | None = None,  # TODO: Ask to HQ for detail.
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.queue_name = queue_name
        self.cu_count = cu_count
        self.platform = platform
        self.enterprise_project_id = enterprise_project_id
        self.feature = feature
        self.resource_mode = resource_mode
        self.charging_mode = charging_mode
        self.description = description
        self.queue_type = queue_type
        self.list_tags_body = list_tags_body
        self.list_labels_body = list_labels_body
        self.elastic_resource_pool_name = elastic_resource_pool_name
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):

        # Connection parameter and kwargs parameter from Airflow UI
        dli_hook = DLIHook(huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        return dli_hook.create_queue(
            project_id=self.project_id,
            queue_name=self.queue_name,
            cu_count=self.cu_count,
            platform=self.platform,
            enterprise_project_id=self.enterprise_project_id,
            feature=self.feature,
            resource_mode=self.resource_mode,
            charging_mode=self.charging_mode,
            description=self.description,
            queue_type=self.queue_type,
            list_tags_body=self.list_tags_body,
            list_labels_body=self.list_labels_body,
            elastic_resource_pool_name=self.elastic_resource_pool_name,
        ).to_json_object()


class DLIUpdateQueueCidrOperator(BaseOperator):
    def __init__(
        self,
        project_id: str,
        queue_name: str,
        cidr_in_vpc: str,
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.queue_name = queue_name
        self.cidr_in_vpc = cidr_in_vpc
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):

        # Connection parameter and kwargs parameter from Airflow UI
        dli_hook = DLIHook(huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        return dli_hook.update_queue_cidr(
            project_id=self.project_id, queue_name=self.queue_name, cidr_in_vpc=self.cidr_in_vpc
        ).to_json_object()


class DLIDeleteQueueOperator(BaseOperator):
    def __init__(
        self,
        project_id: str,
        queue_name: str,
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.queue_name = queue_name
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):

        # Connection parameter and kwargs parameter from Airflow UI
        dli_hook = DLIHook(huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        return dli_hook.delete_queue(project_id=self.project_id, queue_name=self.queue_name).to_json_object()


class DLIListQueuesOperator(BaseOperator):
    def __init__(
        self,
        project_id: str,
        queue_type: str | None = None,
        tags: str | None = None,
        return_billing_info: bool = False,
        return_permission_info: bool = False,
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.queue_type = queue_type
        self.tags = tags
        self.return_billing_info = return_billing_info
        self.return_permission_info = return_permission_info
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):

        # Connection parameter and kwargs parameter from Airflow UI
        dli_hook = DLIHook(huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        list = dli_hook.list_queues(
            project_id=self.project_id,
            queue_type=self.queue_type,
            tags=self.tags,
            return_billing_info=self.return_billing_info,
            return_permission_info=self.return_permission_info,
        )

        return list["queues"]


class DLICreateBatchJobOperator(BaseOperator):
    def __init__(
        self,
        project_id: str,
        file_path: str,
        class_name: str,
        queue_name: str | None = None,
        obs_bucket: str | None = None,
        catalog_name: str | None = None,
        image: str | None = None,
        max_retry_times: int | None = None,
        auto_recovery: bool | None = None,
        spark_version: str | None = None,
        feature: str | None = None,
        num_executors: int | None = None,
        executor_cores: int | None = None,
        executor_memory: str | None = None,
        driver_cores: int | None = None,
        driver_memory: str | None = None,
        name: str | None = None,
        list_conf_body: list[object] | None = None,
        list_groups_body: list[object] | None = None,
        list_resources_body: list[object] | None = None,
        list_modules_body: list[object] | None = None,
        list_files_body: list[object] | None = None,
        list_python_files_body: list[object] | None = None,
        list_jars_body: list[object] | None = None,
        sc_type: str | None = None,
        list_args_body: list[object] | None = None,
        cluster_name: str | None = None,
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.queue_name = queue_name
        self.file_path = file_path
        self.class_name = class_name
        self.obs_bucket = obs_bucket
        self.catalog_name = catalog_name
        self.image = image
        self.max_retry_times = max_retry_times
        self.auto_recovery = auto_recovery
        self.spark_version = spark_version
        self.feature = feature
        self.num_executors = num_executors
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.driver_cores = driver_cores
        self.driver_memory = driver_memory
        self.name = name
        self.list_conf_body = list_conf_body
        self.list_groups_body = list_groups_body
        self.list_resources_body = list_resources_body
        self.list_modules_body = list_modules_body
        self.list_files_body = list_files_body
        self.list_python_files_body = list_python_files_body
        self.list_jars_body = list_jars_body
        self.sc_type = sc_type
        self.list_args_body = list_args_body
        self.cluster_name = cluster_name
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):

        # Connection parameter and kwargs parameter from Airflow UI
        dli_hook = DLIHook(huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        return dli_hook.create_batch_job(
            project_id=self.project_id,
            queue_name=self.queue_name,
            file_path=self.file_path,
            class_name=self.class_name,
            auto_recovery=self.auto_recovery,
            catalog_name=self.catalog_name,
            cluster_name=self.cluster_name,
            driver_cores=self.driver_cores,
            driver_memory=self.driver_memory,
            executor_cores=self.executor_cores,
            executor_memory=self.executor_memory,
            feature=self.feature,
            image=self.image,
            list_args_body=self.list_args_body,
            list_conf_body=self.list_conf_body,
            list_files_body=self.list_files_body,
            list_groups_body=self.list_groups_body,
            list_jars_body=self.list_groups_body,
            list_modules_body=self.list_modules_body,
            list_python_files_body=self.list_python_files_body,
            list_resources_body=self.list_resources_body,
            max_retry_times=self.max_retry_times,
            name=self.name,
            num_executors=self.num_executors,
            obs_bucket=self.obs_bucket,
            sc_type=self.sc_type,
            spark_version=self.spark_version,
        ).to_json_object()


class DLIUploadFilesOperator(BaseOperator):
    def __init__(
        self,
        project_id: str,
        group: str,
        file_path: list[object],
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.group = group
        self.file_path = file_path
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):

        # Connection parameter and kwargs parameter from Airflow UI
        dli_hook = DLIHook(huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        return dli_hook.upload_files(
            project_id=self.project_id, group=self.group, file_path=self.file_path
        ).to_json_object()


class DLIRunjobOperator(BaseOperator):
    def __init__(
        self,
        project_id: str,
        sql_query: str,
        database_name: str | None = None,
        queue_name: str | None = None,
        list_tags_body: list[object] | None = None,
        list_conf_body: list[object] | None = None,
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.sql_query = sql_query
        self.database_name = database_name
        self.queue_name = queue_name
        self.huaweicloud_conn_id = huaweicloud_conn_id
        self.list_tags_body = list_tags_body
        self.list_conf_body = list_conf_body

    def execute(self, context: Context):

        # Connection parameter and kwargs parameter from Airflow UI
        dli_hook = DLIHook(huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        return dli_hook.run_job(
            project_id=self.project_id,
            sql_query=self.sql_query,
            database_name=self.database_name,
            queue_name=self.queue_name,
            list_tags_body=self.list_tags_body,
            list_conf_body=self.list_conf_body,
        ).to_json_object()
