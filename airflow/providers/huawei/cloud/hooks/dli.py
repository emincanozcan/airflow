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

    def create_queue(
        self,
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
        list_tags_body,
        list_labels_body,
        cu_count,
    ) -> DliSdk.CreateQueueResponse:
        if list_tags_body != None and len(list_tags_body) > 10:
            raise AirflowException("You can add up to 10 tags.")
        try:
            return self._get_dli_client(project_id).create_queue(
                self.create_queue_request(
                    elastic_resource_pool_name=elastic_resource_pool_name,
                    list_tags_body=list_tags_body,
                    feature=feature,
                    list_labels_body=list_labels_body,
                    resource_mode=resource_mode,
                    platform=platform,
                    enterprise_project_id=enterprise_project_id,
                    charging_mode=charging_mode,
                    cu_count=cu_count,
                    description=description,
                    queue_type=queue_type,
                    queue_name=queue_name,
                )
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when creating: {e}")

    def update_queue_cidr(self, project_id, queue_name, cidr_in_vpc) -> DliSdk.UpdateQueueCidrResponse:
        try:
            return self._get_dli_client(project_id).update_queue_cidr(
                self.update_queue_cidr_request(queue_name=queue_name, cidr_in_vpc=cidr_in_vpc)
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when updating: {e}")

    def delete_queue(self, project_id, queue_name) -> DliSdk.DeleteQueueResponse:
        try:
            return self._get_dli_client(project_id).delete_queue(self.delete_queue_request(queue_name))
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when deleting: {e}")

    def list_queues(
        self, project_id, queue_type, tags, return_billing_info, return_permission_info
    ) -> DliSdk.ListQueuesResponse:
        try:
            return self._get_dli_client(project_id).list_queues(
                self.list_queues_request(
                    queue_type=queue_type,
                    tags=tags,
                    return_billing_info=return_billing_info,
                    return_permission_info=return_permission_info,
                )
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when listing: {e}")

    def create_batch_job(
        self,
        project_id,
        queue_name,
        file_path,
        class_name,
        obs_bucket,
        catalog_name,
        image,
        max_retry_times,
        auto_recovery,
        spark_version,
        feature,
        num_executors,
        executor_cores,
        executor_memory,
        driver_cores,
        driver_memory,
        name,
        list_conf_body,
        list_groups_body,
        list_resources_body,
        list_modules_body,
        list_files_body,
        list_python_files_body,
        list_jars_body,
        sc_type,
        list_args_body,
        cluster_name,
    ) -> DliSdk.CreateBatchJobResponse:
        try:

            return self._get_dli_client(project_id).create_batch_job(
                self.create_batch_job_request(
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
                    list_conf_body=list_conf_body,
                    list_groups_body=list_groups_body,
                    list_resources_body=list_resources_body,
                    list_modules_body=list_modules_body,
                    list_files_body=list_files_body,
                    list_python_files_body=list_python_files_body,
                    list_jars_body=list_jars_body,
                    sc_type=sc_type,
                    list_args_body=list_args_body,
                    cluster_name=cluster_name
                )
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when crating batch job: {e}")

    def upload_files(self, project_id, file_path, group) -> DliSdk.UploadFilesResponse:
        try:
            return self._get_dli_client(project_id).upload_files(
                self.upload_files_request(file_path=file_path, group=group)
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when uploading files: {e}")

    def run_job(
        self, project_id, sql_query, database_name, queue_name, list_conf_body, list_tags_body
    ) -> DliSdk.RunJobResponse:
        try:
            return self._get_dli_client(project_id).run_job(
                self.run_job_request(
                    sql_query=sql_query,
                    database_name=database_name,
                    list_conf_body=list_conf_body,
                    list_tags_body=list_tags_body,
                    queue_name=queue_name
                )
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when running: {e}")

    def show_batch_state(self, project_id, job_id) -> str:
        try:
            response = self._get_dli_client(project_id).show_batch_state(
                self.show_batch_state_request(job_id)
            )
            return response.state
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when get batch state: {e}")

    def show_job_status(self, project_id, job_id) -> str:
        try:
            response = self._get_dli_client(project_id).show_job_status(self.show_job_status_request(job_id))
            return response.status
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when get job status: {e}")

    def _get_dli_client(self, project_id) -> DliSdk.DliClient:

        ak = self.conn.login
        sk = self.conn.password

        credentials = BasicCredentials(ak, sk, project_id)

        return (
            DliSdk.DliClient.new_builder()
            .with_credentials(credentials)
            .with_region(DliRegion.value_of(self.get_region()))
            .build()
        )

    def show_job_status_request(self, job_id):
        return DliSdk.ShowJobStatusRequest(job_id)

    def show_batch_state_request(self, job_id):
        return DliSdk.ShowBatchStateRequest(job_id)

    def run_job_request(self, sql_query, database_name, queue_name, list_conf_body, list_tags_body):
        request = DliSdk.RunJobRequest()
        request.body = DliSdk.CommitJobReq(
            queue_name=queue_name,
            currentdb=database_name,
            sql=sql_query,
            tags=list_tags_body,
            conf=list_conf_body,
        )
        return request

    def upload_files_request(self, file_path, group):
        request = DliSdk.UploadFilesRequest()
        request.body = DliSdk.UploadGroupPackageReq(group=group, paths=file_path)
        return request

    def create_batch_job_request(
        self,
        queue_name,
        file_path,
        class_name,
        obs_bucket,
        catalog_name,
        image,
        max_retry_times,
        auto_recovery,
        spark_version,
        feature,
        num_executors,
        executor_cores,
        executor_memory,
        driver_cores,
        driver_memory,
        name,
        list_conf_body,
        list_groups_body,
        list_resources_body,
        list_modules_body,
        list_files_body,
        list_python_files_body,
        list_jars_body,
        sc_type,
        list_args_body,
        cluster_name,
    ):
        request = DliSdk.CreateBatchJobRequest()
        request.body = DliSdk.CreateBatchJobReq(
            queue=queue_name,
            file=file_path,
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
            conf=list_conf_body,
            groups=list_groups_body,
            resources=list_resources_body,
            modules=list_modules_body,
            files=list_files_body,
            python_files=list_python_files_body,
            jars=list_jars_body,
            sc_type=sc_type,
            args=list_args_body,
            cluster_name=cluster_name
        )
        return request

    def list_queues_request(self, queue_type, tags, return_billing_info, return_permission_info):
        return DliSdk.ListQueuesRequest(
            queue_type=queue_type,
            tags=tags,
            with_charge_info=return_billing_info,
            with_priv=return_permission_info,
        )

    def delete_queue_request(self, queue_name):
        return DliSdk.DeleteQueueRequest(queue_name)

    def update_queue_cidr_request(self, queue_name, cidr_in_vpc):
        request = DliSdk.UpdateQueueCidrRequest()
        request.queue_name = queue_name
        request.body = DliSdk.UpdateQueueCidrReq(cidr_in_vpc=cidr_in_vpc)
        return request

    def create_queue_request(
        self,
        queue_name,
        platform,
        enterprise_project_id,
        elastic_resource_pool_name,
        feature,
        resource_mode,
        charging_mode,
        description,
        queue_type,
        list_tags_body,
        list_labels_body,
        cu_count,
    ):
        request = DliSdk.CreateQueueRequest()
        request.body = DliSdk.CreateQueueReq(
            elastic_resource_pool_name=elastic_resource_pool_name,
            tags=list_tags_body,
            feature=feature,
            labels=list_labels_body,
            resource_mode=resource_mode,
            platform=platform,
            enterprise_project_id=enterprise_project_id,
            charging_mode=charging_mode,
            cu_count=cu_count,
            description=description,
            queue_type=queue_type,
            queue_name=queue_name,
        )
        return request
