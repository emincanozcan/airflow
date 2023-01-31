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
from airflow.providers.huawei.cloud.operators.huawei_obs import (
    OBSGetBucketTaggingOperator,
    OBSSetBucketTaggingOperator,
    OBSDeleteBucketTaggingOperator
)

MOCK_TASK_ID = "test-obs-operator"
MOCK_REGION = "mock_region"
MOCK_OBS_CONN_ID = "mock_obs_conn_default"
MOCK_BUCKET_NAME = "mock_bucket_name"
MOCK_TAG_INGO = [{"mock_key1": "mock_value1"}, {"mock_key2": "mock_value2"}]
MOCK_CONTEXT = mock.Mock()


class TestOBSGetBucketTaggingOperator(unittest.TestCase):
    def setUp(self):
        self.operator = OBSGetBucketTaggingOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_OBS_CONN_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET_NAME,
        )

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.ObsHook")
    def test_execute_if_bucket_exist(self, mock_hook):
        mock_hook.return_value.exist_bucket.return_value = True

        self.operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.exist_bucket.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_hook.return_value.get_bucket_tagging.assert_called_once_with(MOCK_BUCKET_NAME)

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.ObsHook")
    def test_execute_if_not_bucket_exist(self, mock_hook):
        mock_hook.return_value.exist_bucket.return_value = False
        mock_hook.return_value.region = MOCK_REGION

        self.operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.exist_bucket.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_hook.return_value.get_bucket_tagging.assert_not_called()


class TestOBSSetBucketTaggingOperator(unittest.TestCase):
    def setUp(self):
        self.operator = OBSSetBucketTaggingOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_OBS_CONN_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET_NAME,
            tag_info=MOCK_TAG_INGO,
        )

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.ObsHook")
    def test_execute_if_bucket_exist(self, mock_hook):
        mock_hook.return_value.exist_bucket.return_value = True

        self.operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.exist_bucket.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_hook.return_value.set_bucket_tagging.assert_called_once_with(
            bucket_name=MOCK_BUCKET_NAME,
            tag_info=MOCK_TAG_INGO
        )

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.ObsHook")
    def test_execute_if_not_bucket_exist(self, mock_hook):
        mock_hook.return_value.exist_bucket.return_value = False
        mock_hook.return_value.region = MOCK_REGION

        self.operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.exist_bucket.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_hook.return_value.set_bucket_tagging.assert_not_called()


class TestOBSDeleteBucketTaggingOperator(unittest.TestCase):
    def setUp(self):
        self.operator = OBSDeleteBucketTaggingOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_OBS_CONN_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET_NAME,
        )

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.ObsHook")
    def test_execute_if_bucket_exist(self, mock_hook):
        mock_hook.return_value.exist_bucket.return_value = True

        self.operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.exist_bucket.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_hook.return_value.delete_bucket_tagging.assert_called_once_with(
            bucket_name=MOCK_BUCKET_NAME,
        )

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.ObsHook")
    def test_execute_if_not_bucket_exist(self, mock_hook):
        mock_hook.return_value.exist_bucket.return_value = False
        mock_hook.return_value.region = MOCK_REGION

        self.operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.exist_bucket.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_hook.return_value.delete_bucket_tagging.assert_not_called()



