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

import unittest
from unittest import mock
from unittest.mock import PropertyMock

from airflow.providers.huawei.cloud.sensors.dataarts import DataArtsDLFShowJobStatusSensor

DLF_SENSOR_STRING = "airflow.providers.huawei.cloud.sensors.dataarts.{}"
MOCK_JOB_NAME = "test-job"
MOCK_PROJECT_ID = "test-project"
MOCK_CONN_ID = "huaweicloud_default"
MOCK_TASK_ID = "test-dlf-operator"
MOCK_WORKSPACE = "workspace-id"
MOCK_REGION = "region"


class TestDataArtsDLFShowJobStatusSensor(unittest.TestCase):
    def setUp(self):
        self.job_status_sensor = DataArtsDLFShowJobStatusSensor(
            task_id=MOCK_TASK_ID,
            workspace=MOCK_WORKSPACE,
            project_id=MOCK_PROJECT_ID,
            job_name=MOCK_JOB_NAME,
            huaweicloud_conn_id=MOCK_CONN_ID,
            region=MOCK_REGION,
        )

    @mock.patch(DLF_SENSOR_STRING.format("DataArtsHook"))
    def test_get_hook(self, mock_service):
        self.job_status_sensor.get_hook()
        mock_service.assert_called_once_with(
            huaweicloud_conn_id=MOCK_CONN_ID, project_id=MOCK_PROJECT_ID, region=MOCK_REGION
        )

    @mock.patch(
        DLF_SENSOR_STRING.format("DataArtsDLFShowJobStatusSensor.get_hook"), new_callable=PropertyMock
    )
    def test_poke_show_job_status_starting(self, mock_service):
        # Given
        mock_service.return_value.dlf_show_job_status.return_value = "STARTING"

        # When
        res = self.job_status_sensor.poke(None)

        # Then
        assert res is False
        mock_service.return_value.dlf_show_job_status.assert_called_once_with(
            job_name=MOCK_JOB_NAME, workspace=MOCK_WORKSPACE
        )

    @mock.patch(
        DLF_SENSOR_STRING.format("DataArtsDLFShowJobStatusSensor.get_hook"), new_callable=PropertyMock
    )
    def test_poke_show_job_status_stopped_success(self, mock_service):
        # Given
        mock_service.return_value.dlf_show_job_status.return_value = "STOPPED"
        mock_service.return_value.dlf_list_job_instances.return_value = [mock_node(MOCK_JOB_NAME, "success")]

        # When
        res = self.job_status_sensor.poke(None)

        # Then
        assert res is True
        mock_service.return_value.dlf_list_job_instances.assert_called_once_with(workspace=MOCK_WORKSPACE)

    @mock.patch(
        DLF_SENSOR_STRING.format("DataArtsDLFShowJobStatusSensor.get_hook"), new_callable=PropertyMock
    )
    def test_poke_show_job_status_stopped_running(self, mock_service):
        # Given
        mock_service.return_value.dlf_show_job_status.return_value = "STOPPED"
        mock_service.return_value.dlf_list_job_instances.return_value = [mock_node(MOCK_JOB_NAME, "running")]

        # When
        res = self.job_status_sensor.poke(None)

        # Then
        assert res is False
        mock_service.return_value.dlf_list_job_instances.assert_called_once_with(workspace=MOCK_WORKSPACE)

class mock_node:
    def __init__(self, job_name, status):
        self.job_name = job_name
        self.status = status
