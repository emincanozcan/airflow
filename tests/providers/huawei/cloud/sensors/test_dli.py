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

from airflow.providers.huawei.cloud.sensors.dli import DLISqlShowJobStatusSensor, DLISparkShowBatchStateSensor

DLI_SENSOR_STRING = "airflow.providers.huawei.cloud.sensors.dli.{}"
MOCK_JOB_ID = "test-job"
MOCK_PROJECT_ID = "test-project"
MOCK_CONN_ID = "huaweicloud_default"
MOCK_REGION = "mock_region"
MOCK_TASK_ID = "test-dli-operator"

class TestDLISensor(unittest.TestCase):
    def setUp(self):
        self.job_status_sensor = DLISqlShowJobStatusSensor(
            task_id=MOCK_TASK_ID,
            job_id=MOCK_JOB_ID,
            project_id=MOCK_PROJECT_ID,
            huaweicloud_conn_id=MOCK_CONN_ID,
        )
        self.batch_state_sensor = DLISparkShowBatchStateSensor(
            task_id=MOCK_TASK_ID,
            job_id=MOCK_JOB_ID,
            project_id=MOCK_PROJECT_ID,
            huaweicloud_conn_id=MOCK_CONN_ID,
        )

    @mock.patch(DLI_SENSOR_STRING.format("DLIHook"))
    def test_get_hook(self, mock_service):
        self.job_status_sensor.get_hook()
        mock_service.assert_called_once_with(huaweicloud_conn_id=MOCK_CONN_ID, project_id=MOCK_PROJECT_ID)

    @mock.patch(DLI_SENSOR_STRING.format("DLISqlShowJobStatusSensor.get_hook"), new_callable=PropertyMock)
    def test_poke_show_job_status(self, mock_service):
        # Given
        mock_service.return_value.show_job_status.return_value = True

        # When
        res = self.job_status_sensor.poke(None)

        # Then
        assert res is True
        mock_service.return_value.show_job_status.assert_called_once_with(
            job_id=MOCK_JOB_ID
        )

    @mock.patch(DLI_SENSOR_STRING.format("DLISparkShowBatchStateSensor.get_hook"), new_callable=PropertyMock)
    def test_poke_show_batch_state(self, mock_service):
        # Given
        mock_service.return_value.show_batch_state.return_value = True

        # When
        res = self.batch_state_sensor.poke(None)

        # Then
        assert res is True
        mock_service.return_value.show_batch_state.assert_called_once_with(
            job_id=MOCK_JOB_ID
        )
