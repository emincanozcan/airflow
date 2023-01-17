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
from huaweicloudsdkdli.v1.region.dli_region import DliRegion
import huaweicloudsdkdli.v1 as DliSdk


class DLIHook(HuaweiBaseHook):
    """Interact with Huawei Cloud DLI, using the huaweicloudsdkdli library."""

    def create_queue(self, project_id, queue_name, cu_count):
        try:
            request = DliSdk.CreateQueueRequest()
            request.body = DliSdk.CreateQueueReq(
                cu_count=cu_count,
                queue_name=queue_name,
                queue_type="general"  # Static or Dynamic ?
            )
            response = self._get_dli_client(project_id).create_queue(request)
            print(response)
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when creating: {e}")
    
    def delete_queue(self, project_id, queue_name):
        try:
            request = DliSdk.DeleteQueueRequest()
            request.queue_name = queue_name
            response = self._get_dli_client(project_id).delete_queue(request)
            print(response)
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when deleting: {e}")
    
    def list_queue(self,project_id):
        try:
            request = DliSdk.ListQueuesRequest()
            request.queue_type = "general" # Static or Dynamic ?
            response = self._get_dli_client(project_id).list_queues(request)
            print(response)
            return response
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when listing: {e}")
    
    def create_batch_job(self,project_id, queue_name, file_path):
        try:
            request = DliSdk.CreateBatchJobRequest()
            request.body = DliSdk.CreateBatchJobReq(
                queue=queue_name,
                file=file_path
            )
            response = self._get_dli_client(project_id).create_batch_job(request)
            print(response)
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when crating batch job: {e}")
    
    def upload_files(self, project_id, file_path, group):
        try:
            request = DliSdk.UploadFilesRequest()
            listPathsbody = file_path
            request.body = DliSdk.UploadGroupPackageReq(
                group=group,
                paths=listPathsbody
            )
            response = self._get_dli_client(project_id).upload_files(request)
            print(response)
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when uploading files: {e}")
    
    def run_job(self, project_id, sql_query, database_name, queue_name):
        try:
            request = DliSdk.RunJobRequest()
            request.body = DliSdk.CommitJobReq(
                queue_name=queue_name,
                currentdb=database_name,
                sql=sql_query
            )
            response = self._get_dli_client(project_id).run_job(request)
            print(response)
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when running: {e}")

    def show_batch_state(self, project_id, job_id):
        current_state = self.check_batch_state(project_id, job_id)
        return current_state
    
    def show_job_status(self, project_id, job_id):
        current_status = self.check_job_status(project_id, job_id)
        return current_status
    
    def check_batch_state(self, project_id, job_id):
        try:
            request = DliSdk.ShowBatchStateRequest()
            request.batch_id = job_id
            response = self._get_dli_client(project_id).show_batch_state(request)
            return response.state
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when get batch state: {e}")
    
    def check_job_status(self, project_id, job_id):
        try:
            request = DliSdk.ShowJobStatusRequest()
            request.job_id = job_id
            response = self._get_dli_client(project_id).show_job_status(request)
            return response.status
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when get job status: {e}")

    def _get_dli_client(self, project_id) -> DliSdk.DliClient:

        ak = self.conn.login
        sk = self.conn.password

        credentials = BasicCredentials(ak, sk, project_id)

        return DliSdk.DliClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(DliRegion.value_of(self.get_region())) \
            .build()

