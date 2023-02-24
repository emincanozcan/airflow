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
"""This module contains Huawei Cloud DLF operators."""
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.huawei.cloud.hooks.dataarts import DataArtsHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

class DataArtsDLFStartJobOperator(BaseOperator):
    """
    This operator is used to start a job.

    :param project_id: The ID of the project.
    :param workspace: Workspace ID. If this parameter is not set, data in the default workspace is queried by default.
        To query data in other workspaces, this header must be carried.
    :param job_name: Job name.
    :param body: The body of the request.
    :param region: The name of the region.
    :param huaweicloud_conn_id: The connection ID to use when fetching connection info.
    """
    def __init__(
        self,
        job_name: str,
        project_id: str | None = None,
        workspace: str | None = None,
        body: dict | None = None,
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.workspace = workspace
        self.job_name = job_name
        self.body = body
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context):

        # Connection parameter and kwargs parameter from Airflow UI
        dataarts_hook = DataArtsHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region, project_id=self.project_id)

        dataarts_hook.dlf_start_job(
            workspace=self.workspace,
            job_name=self.job_name,
            body=self.body
        )
