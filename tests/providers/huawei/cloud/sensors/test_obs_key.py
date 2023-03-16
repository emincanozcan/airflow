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

from airflow.providers.huawei.cloud.sensors.huawei_obs_key import OBSObjectKeySensor


OBS_SENSOR_STRING = "airflow.providers.huawei.cloud.sensors.huawei_obs_key.{}"
MOCK_CONN_ID = "huaweicloud_default"
MOCK_REGION = "mock_region"
MOCK_TASK_ID = "test-obs-operator"
MOCK_BUCKET = "mock_bucket"
MOCK_OBJECT_KEY = "mock_object_key"
MOCK_OBJECT_KEY_2 = "mock_object_key_2"
MOCK_STATUS = "AVAILABLE"

class TestOBSObjectKeySensor(unittest.TestCase):
    def setUp(self):
        self.sensor = OBSObjectKeySensor(
            task_id=MOCK_TASK_ID,
            bucket_name=MOCK_BUCKET,
            object_key=MOCK_OBJECT_KEY,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_CONN_ID,
        )

    @mock.patch(OBS_SENSOR_STRING.format("OBSHook"))
    def test_get_hook(self, mock_service):
        self.sensor.get_hook()
        mock_service.assert_called_once_with(huaweicloud_conn_id=MOCK_CONN_ID, region=MOCK_REGION)

    @mock.patch(OBS_SENSOR_STRING.format("OBSHook"))
    def test_check_object_key(self, mock_service):
        mock_service.get_obs_bucket_object_key.return_value = (MOCK_BUCKET, MOCK_OBJECT_KEY)
        self.sensor.get_hook()
        self.sensor.hook.exist_bucket.return_value = True
        self.sensor.hook.exist_object.return_value = True

        res = self.sensor._check_object_key(MOCK_OBJECT_KEY)

        self.assertEqual(True, res)

    @mock.patch(OBS_SENSOR_STRING.format("OBSObjectKeySensor._check_object_key"))
    def test_poke_if_object_key_not_exist(self, mock_check):
        mock_check.return_value = False

        res = self.sensor.poke(None)

        self.assertEqual(False, res)

    @mock.patch(OBS_SENSOR_STRING.format("OBSObjectKeySensor._check_object_key"))
    def test_poke_if_object_key_exist(self, mock_check):
        mock_check.return_value = True

        res = self.sensor.poke(None)

        self.assertEqual(True, res)

    @mock.patch(OBS_SENSOR_STRING.format("OBSObjectKeySensor._check_object_key"))
    def test_poke_if_object_keys_has_not_exist(self, mock_check):
        self.sensor.object_key = [MOCK_OBJECT_KEY, MOCK_OBJECT_KEY_2]
        mock_check.side_effect = [True, False]

        res = self.sensor.poke(None)

        self.assertEqual(False, res)

