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
import huaweicloudsdkdlf.v1 as DlfSdk
from huaweicloudsdkdlf.v1.region.dlf_region import DlfRegion

class DataArtsHook(HuaweiBaseHook):
    
    def dlf_start_job(self, project_id, workspace, job_name, body):
        try:
            return self._get_dlf_client(project_id=project_id).start_job(self.dlf_start_job_request(workspace, job_name, body))
        except Exception as e:
            raise AirflowException(f"Errors when starting: {e}")
    
    def dlf_show_job_status(self, project_id, workspace, job_name):
        try:
            return self._get_dlf_client(project_id=project_id).show_job_status(self.dlf_show_job_status_request(workspace, job_name))
        except Exception as e:
            raise AirflowException(f"Errors when showing job status: {e}")

    def _get_dlf_client(self, project_id) -> DlfSdk.DlfClient:

        ak = self.conn.login
        sk = self.conn.password

        credentials = BasicCredentials(ak, sk, project_id)

        return (
            DlfSdk.DlfClient.new_builder()
            .with_credentials(credentials)
            .with_region(DlfRegion.value_of(self.get_region()))
            .build()
        )
    
    def dlf_start_job_request(self, workspace, job_name, body):
        request = DlfSdk.StartJobRequest(workspace=workspace, job_name=job_name, body=body)
        return request  
    
    def dlf_show_job_status_request(self, workspace, job_name):
        request = DlfSdk.ShowJobStatusRequest(workspace=workspace, job_name=job_name)
        return request