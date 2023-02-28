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

from airflow.providers.huawei.cloud.sensors.cdm import CDMShowJobStatusSensor

CDM_SENSOR_STRING = "airflow.providers.huawei.cloud.sensors.cdm.{}"
MOCK_JOB_NAME = "test-job"
MOCK_PROJECT_ID = "test-project"
MOCK_CONN_ID = "huaweicloud_default"
MOCK_TASK_ID = "test-cdm-operator"
MOCK_CLUSTER_ID = "cluster-id"
MOCK_REGION = "region"

MOCK_STATUS_RESPONSE = {
    "submissions": [
        {
            "job-name": "jdbc2hive",
            "creation-user": "cdm",
            "creation-date": "1536905778725",
            "progress": 1,
            "status": "BOOTING"
        }
    ]
}


class TestCDMSensor(unittest.TestCase):
    def setUp(self):
        self.job_status_sensor = CDMShowJobStatusSensor(
            task_id=MOCK_TASK_ID,
            cluster_id=MOCK_CLUSTER_ID,
            project_id=MOCK_PROJECT_ID,
            job_name=MOCK_JOB_NAME,
            huaweicloud_conn_id=MOCK_CONN_ID,
            region=MOCK_REGION
        )

    @mock.patch(CDM_SENSOR_STRING.format("CDMHook"))
    def test_get_hook(self, mock_service):
        self.job_status_sensor.get_hook()
        mock_service.assert_called_once_with(huaweicloud_conn_id=MOCK_CONN_ID, project_id=MOCK_PROJECT_ID, region=MOCK_REGION)

    @mock.patch(CDM_SENSOR_STRING.format("CDMShowJobStatusSensor.get_hook"), new_callable=PropertyMock)
    def test_poke_show_job_status(self, mock_service):
        # Given
        mock_service.return_value.show_job_status.return_value = MOCK_STATUS_RESPONSE["submissions"]
        # assert mock_service.return_value.show_job_status.return_value is 123
        # When
        res = self.job_status_sensor.poke(None)

        # Then
        assert res is False
        mock_service.return_value.show_job_status.assert_called_once_with(
            job_name=MOCK_JOB_NAME, cluster_id=MOCK_CLUSTER_ID
        )
