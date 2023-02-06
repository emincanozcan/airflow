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

from typing import Any
from airflow.exceptions import AirflowException
from airflow.providers.huawei.cloud.hooks.base_huawei_cloud import HuaweiBaseHook

from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdkcdm.v1.region.cdm_region import CdmRegion
import huaweicloudsdkcdm.v1 as CdmSdk


class CDMHook(HuaweiBaseHook):
    """Interact with Huawei Cloud CDM, using the huaweicloudsdkcdm library."""

    def create_job(
        self,
        project_id,
        cluster_id,
        jobs
    ) -> CdmSdk.CreateJobResponse:
        try:
            return self._get_cdm_client(project_id).create_job(self.create_job_request(cluster_id=cluster_id, jobs=jobs))
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when creating job: {e}")

    def create_and_execute_job(
        self,
        project_id,
        x_language,
        clusters,
        jobs
    ) -> CdmSdk.CreateAndStartRandomClusterJobResponse:
        try:
            return self._get_cdm_client(project_id).create_and_start_random_cluster_job(
                self.create_and_execute_job_request(x_language=x_language,
                    clusters=clusters, jobs=jobs
                ))
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when creating job: {e}")

    def start_job(self, project_id, cluster_id, job_name) -> CdmSdk.StartJobResponse:
        try:
            return self._get_cdm_client(project_id).start_job(self.start_job_request(cluster_id=cluster_id, job_name=job_name))
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when starting job: {e}")

    def delete_job(self, project_id, cluster_id, job_name) -> CdmSdk.DeleteJobResponse:
        try:
            return self._get_cdm_client(project_id).delete_job(self.delete_job_request(cluster_id=cluster_id, job_name=job_name))
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when deleting job: {e}")

    def stop_job(self, project_id, cluster_id, job_name) -> CdmSdk.StopJobResponse:
        try:
            return self._get_cdm_client(project_id).stop_job(self.stop_job_request(cluster_id=cluster_id, job_name=job_name))
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when stopping job: {e}")

    def show_job_status(self, project_id, cluster_id, job_name) -> CdmSdk.ShowJobStatusResponse:
        try:
            response = self._get_cdm_client(project_id).show_job_status(
                self.show_job_status_request(cluster_id=cluster_id, job_name=job_name))
            return response.submissions
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when get job status: {e}")

    def create_job_request(
        self,
        cluster_id: str,
        jobs: list[dict]
    ):
        request = CdmSdk.CreateJobRequest(
            cluster_id=cluster_id, body=CdmSdk.CdmCreateJobJsonReq(jobs))
        return request

    def create_and_execute_job_request(
        self,
        x_language: str,
        clusters: list,
        jobs: list[dict]
    ):
        request_body = {"jobs": jobs, "clusters": clusters}
        request = CdmSdk.CreateAndStartRandomClusterJobRequest(
            x_language=x_language,
            body=request_body)
        return request

    def start_job_request(self, cluster_id, job_name):
        request = CdmSdk.StartJobRequest()
        request.cluster_id = cluster_id
        request.job_name = job_name
        return request

    def delete_job_request(self, cluster_id, job_name):
        request = CdmSdk.DeleteJobRequest()
        request.cluster_id = cluster_id
        request.job_name = job_name
        return request

    def stop_job_request(self, cluster_id, job_name):
        request = CdmSdk.StopJobRequest()
        request.cluster_id = cluster_id
        request.job_name = job_name
        return request

    def show_job_status_request(self, cluster_id, job_name):
        request = CdmSdk.ShowJobStatusRequest()
        request.cluster_id = cluster_id
        request.job_name = job_name
        return request

    def _get_cdm_client(self, project_id) -> CdmSdk.CdmClient:

        ak = self.conn.login
        sk = self.conn.password

        credentials = BasicCredentials(ak, sk, project_id)

        return (
            CdmSdk.CdmClient.new_builder()
            .with_credentials(credentials)
            .with_region(CdmRegion.value_of(self.get_region()))
            .build()
        )
