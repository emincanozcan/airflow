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
from airflow.providers.huawei.cloud.hooks.dlf import DLFHook
from airflow.sensors.base import BaseSensorOperator


class DLFShowJobStatusSensor(BaseSensorOperator):
    """
    Used to view running status of a real-time job
    
    :param job_name: The name of the job.
    :type job_name: str
    :param project_id: The ID of the project.
    :type project_id: str
    :param workspace: The name of the workspace.
    :type workspace: str
    :param huaweicloud_conn_id: The connection ID to use when fetching connection info.
    :type huaweicloud_conn_id: str
    """
    
    #Status of a job, including STARTING, NORMAL, EXCEPTION, STOPPING, STOPPED.
    
    INTERMEDIATE_STATES = (
        "STARTING",
        "NORMAL",
        "LAUNCHING",
        "STOPPING"
    )
    FAILURE_STATES = (
        "EXCEPTION",
    )
    SUCCESS_STATES = ("STOPPED",)

    def __init__(
        self,
        *,
        job_name: str,
        project_id: str,
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
        
        state = self.get_hook.show_job_status(job_name=self.job_name, workspace=self.workspace, project_id=self.project_id)
        if state in self.FAILURE_STATES:
            raise AirflowException("DLF sensor failed")

        if state in self.INTERMEDIATE_STATES:
            return False
        return True

    @cached_property
    def get_hook(self) -> DLFHook:
        """Create and return a DLFHook"""
        return DLFHook(huaweicloud_conn_id=self.huaweicloud_conn_id)