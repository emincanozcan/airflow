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

from typing import TYPE_CHECKING, Any, Sequence

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.providers.huawei.cloud.hooks.dataarts import DataArtsHook
from airflow.sensors.base import BaseSensorOperator


class DataArtsDLFShowJobStatusSensor(BaseSensorOperator):
    """
    Used to view running status of a real-time job
    
    :param job_name: The name of the job.
    :param project_id: The ID of the project.
    :param workspace: The name of the workspace.
    :param huaweicloud_conn_id: The connection ID to use when fetching connection info.
    """
    
    #Status of a job, including STARTING, NORMAL, EXCEPTION, STOPPING, STOPPED.
    #Status of a job instance, including waiting, running, success, fail, running-exception, pause, manual-stop
    
    END_STATES = (
        "STOPPED", 
        "EXCEPTION"
    )
    
    #CONTINIOUS_STATES = "STARTING"
    
    INTERMEDIATE_STATES = (
        "waiting",
        "running",
        "pause",
        "manual-stop"
    )
    FAILURE_STATES = (
        "fail",
        "running-exception"
    )
    SUCCESS_STATES = ("success",)

    def __init__(
        self,
        *,
        job_name: str,
        project_id: str | None = None,
        workspace: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.huaweicloud_conn_id = huaweicloud_conn_id
        self.job_name = job_name
        self.workspace = workspace
        self.project_id = project_id

    def poke(self, context: Context) -> bool:
        """
        Query the job status.
        @param self - the object itself
        @param context - the context of the object
        @returns True if the job status stopped, False otherwise
        """
        
        status = self.get_hook.dlf_show_job_status(job_name=self.job_name, workspace=self.workspace)
        print(f"job state: {status}")
        if status not in self.END_STATES:
            return False
            
        """ if state == self.CONTINIOUS_STATES:
            return False """
        
        instances = self.get_hook.dlf_list_job_instances(workspace=self.workspace)
        print(f"instances count: {instances}")
        #raise AirflowException(instances)
        for instance in instances:
            #print(instance)
            if instance.job_name == self.job_name:
                print(f"instance state: {instance.status}")
                if instance.status in self.FAILURE_STATES:
                    raise AirflowException("DataArts DLF sensor failed")

                if instance.status in self.INTERMEDIATE_STATES:
                    return False
                
                return True
        """ if state in self.FAILURE_STATES:
            raise AirflowException("DataArts DLF sensor failed")

        if state in self.INTERMEDIATE_STATES:
            return False """
        #raise AirflowException("DataArts DLF job not found")
        return False
    
    @cached_property
    def get_hook(self) -> DataArtsHook:
        """Create and return a DataArtsHook"""
        return DataArtsHook(huaweicloud_conn_id=self.huaweicloud_conn_id, project_id=self.project_id)