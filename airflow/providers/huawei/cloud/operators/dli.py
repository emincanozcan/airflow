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
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.queue_name = queue_name
        self.cu_count = cu_count
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):

        # Connection parameter and kwargs parameter from Airflow UI
        dli_hook = DLIHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        dli_hook.create_queue(project_id=self.project_id,
                              queue_name=self.queue_name, cu_count=self.cu_count)


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
        dli_hook = DLIHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        dli_hook.delete_queue(project_id=self.project_id,
                              queue_name=self.queue_name)


class DLIListQueueOperator(BaseOperator):

    def __init__(
        self,
        project_id: str,
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):

        # Connection parameter and kwargs parameter from Airflow UI
        dli_hook = DLIHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        dli_hook.list_queue(project_id=self.project_id)


class DLICreateBatchJobOperator(BaseOperator):

    def __init__(
        self,
        project_id: str,
        queue_name: str,
        file_path: str,
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.queue_name = queue_name
        self.file_path = file_path
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):

        # Connection parameter and kwargs parameter from Airflow UI
        dli_hook = DLIHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        dli_hook.create_batch_job(
            project_id=self.project_id, queue_name=self.queue_name, file_path=self.file_path)


class DLIUploadFilesOperator(BaseOperator):

    def __init__(
        self,
        project_id: str,
        group: str,
        file_path: dict,
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
        dli_hook = DLIHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        dli_hook.upload_files(project_id=self.project_id,
                              group=self.group, file_path=self.file_path)


class DLIRunjobOperator(BaseOperator):

    def __init__(
        self,
        project_id: str,
        sql_query: str,
        database_name: str | None = None,
        queue_name: str | None = None,
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

    def execute(self, context: Context):

        # Connection parameter and kwargs parameter from Airflow UI
        dli_hook = DLIHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        dli_hook.run_job(project_id=self.project_id, sql_query=self.sql_query,
                         database_name=self.database_name, queue_name=self.queue_name)
