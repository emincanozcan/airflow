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
from airflow.providers.huawei.cloud.hooks.cdm import CDMHook
from airflow.providers.http.hooks.http import HttpHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CDMCreateJobOperator(BaseOperator):

    def __init__(
        self,
        project_id: str,
        cluster_id: str,
        jobs: list,
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.cluster_id = cluster_id
        self.jobs = jobs
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context):

        # Connection parameter and kwargs parameter from Airflow UI
        cdm_hook = CDMHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        return cdm_hook.create_job(
            project_id=self.project_id,
            cluster_id=self.cluster_id,
            jobs=self.jobs
        ).to_json_object()


class CDMCreateAndExecuteJobOperator(BaseOperator):

    def __init__(
        self,
        project_id: str,
        x_language: str,
        clusters: list,
        jobs: list[dict],
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.x_language = x_language
        self.clusters = clusters
        self.jobs = jobs
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context):

        # Connection parameter and kwargs parameter from Airflow UI
        cdm_hook = CDMHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        return cdm_hook.create_and_execute_job(
            project_id=self.project_id,
            x_language=self.x_language,
            clusters=self.clusters,
            jobs=self.jobs
        ).to_json_object()


class CDMStartJobOperator(BaseOperator):

    def __init__(
        self,
        project_id: str,
        cluster_id: str,
        job_name: str,
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.cluster_id = cluster_id
        self.job_name = job_name
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context):

        # Connection parameter and kwargs parameter from Airflow UI
        cdm_hook = CDMHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        return cdm_hook.start_job(
            project_id=self.project_id,
            cluster_id=self.cluster_id,
            job_name=self.job_name
        ).to_json_object()


class CDMDeleteJobOperator(BaseOperator):

    def __init__(
        self,
        project_id: str,
        cluster_id: str,
        job_name: str,
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.cluster_id = cluster_id
        self.job_name = job_name
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context):

        # Connection parameter and kwargs parameter from Airflow UI
        cdm_hook = CDMHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        return cdm_hook.delete_job(
            project_id=self.project_id,
            cluster_id=self.cluster_id,
            job_name=self.job_name
        ).to_json_object()


class CDMStopJobOperator(BaseOperator):

    def __init__(
        self,
        project_id: str,
        cluster_id: str,
        job_name: str,
        region: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.cluster_id = cluster_id
        self.job_name = job_name
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context):

        # Connection parameter and kwargs parameter from Airflow UI
        cdm_hook = CDMHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        return cdm_hook.stop_job(
            project_id=self.project_id,
            cluster_id=self.cluster_id,
            job_name=self.job_name
        ).to_json_object()
