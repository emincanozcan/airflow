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

from airflow.providers.huawei.cloud.sensors.dws import DWSClusterSensor, DWSSnapshotSensor

DWS_SENSOR_STRING = "airflow.providers.huawei.cloud.sensors.dws.{}"
MOCK_PROJECT_ID = "test-project"
MOCK_CONN_ID = "huaweicloud_default"
MOCK_REGION = "mock_region"
MOCK_TASK_ID = "test-dws-operator"
MOCK_CLUSTER = "mock_cluster"
MOCK_SNAPSHOT = "mock_snapshot"
MOCK_STATUS = "AVAILABLE"


class TestDWSClusterSensor(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster_sensor = DWSClusterSensor(
            task_id=MOCK_TASK_ID,
            cluster_name=MOCK_CLUSTER,
            target_status=MOCK_STATUS,
            region=MOCK_REGION,
            project_id=MOCK_PROJECT_ID,
            huaweicloud_conn_id=MOCK_CONN_ID,
        )

    @mock.patch(DWS_SENSOR_STRING.format("DWSHook"))
    def test_get_hook(self, mock_service):
        self.cluster_sensor.get_hook()
        mock_service.assert_called_once_with(
            huaweicloud_conn_id=MOCK_CONN_ID, project_id=MOCK_PROJECT_ID, region=MOCK_REGION
        )

    @mock.patch(DWS_SENSOR_STRING.format("DWSClusterSensor.get_hook"), new_callable=PropertyMock)
    def test_poke(self, mock_service):
        expect_status = "AVAILABLE"
        mock_service.return_value.get_cluster_status.return_value = expect_status

        res = self.cluster_sensor.poke(None)

        self.assertEqual(expect_status == MOCK_STATUS, res)


class TestDWSSnapshotSensor(unittest.TestCase):
    def setUp(self) -> None:
        self.snapshot_sensor = DWSSnapshotSensor(
            task_id=MOCK_TASK_ID,
            snapshot_name=MOCK_SNAPSHOT,
            target_status=MOCK_STATUS,
            region=MOCK_REGION,
            project_id=MOCK_PROJECT_ID,
            huaweicloud_conn_id=MOCK_CONN_ID,
        )

    @mock.patch(DWS_SENSOR_STRING.format("DWSHook"))
    def test_get_hook(self, mock_service):
        self.snapshot_sensor.get_hook()
        mock_service.assert_called_once_with(
            huaweicloud_conn_id=MOCK_CONN_ID, project_id=MOCK_PROJECT_ID, region=MOCK_REGION
        )

    @mock.patch(DWS_SENSOR_STRING.format("DWSSnapshotSensor.get_hook"), new_callable=PropertyMock)
    def test_poke(self, mock_service):
        expect_status = "AVAILABLE"
        mock_service.return_value.get_snapshot_status.return_value = expect_status

        res = self.snapshot_sensor.poke(None)

        self.assertEqual(expect_status == MOCK_STATUS, res)
