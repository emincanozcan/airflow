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
    OBSCopyObjectOperator,
    OBSCreateObjectOperator,
    OBSDeleteBatchObjectOperator,
    OBSDeleteObjectOperator,
    OBSGetObjectOperator,
    OBSMoveObjectOperator,
)

MOCK_TASK_ID = "test-obs-operator"
MOCK_REGION = "mock_region"
MOCK_OBS_CONN_ID = "mock_obs_conn_default"
MOCK_BUCKET_NAME = "mock_bucket_name"
MOCK_OBJECT_KEY = "mock_object_key"
MOCK_DATA = mock.Mock()
MOCK_OBJECT_TYPE = mock.Mock()
MOCK_CONTEXT = mock.Mock()
MOCK_METADATA = mock.Mock()

MOCK_DOWNLOAD_PATH = "mock_download_path"


class TestOBSOBSCreateObjectOperator(unittest.TestCase):
    def setUp(self):
        self.headers = {
            "md5": None,
            "acl": None,
            "encryption": None,
            "key": None,
            "storageClass": None,
            "expires": None,
        }

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.OBSHook")
    def test_execute_if_bucket_provided(self, mock_hook):
        operator = OBSCreateObjectOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_OBS_CONN_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET_NAME,
            data=MOCK_DATA,
            object_type=MOCK_OBJECT_TYPE,
            object_key=MOCK_OBJECT_KEY,
            metadata=MOCK_METADATA,
        )

        operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.create_object.assert_called_once_with(
            bucket_name=MOCK_BUCKET_NAME,
            object_key=MOCK_OBJECT_KEY,
            object_type=MOCK_OBJECT_TYPE,
            data=MOCK_DATA,
            metadata=MOCK_METADATA,
            headers=self.headers,
        )

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.OBSHook")
    def test_execute_if_bucket_not_provided(self, mock_hook):
        operator = OBSCreateObjectOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_OBS_CONN_ID,
            region=MOCK_REGION,
            data=MOCK_DATA,
            object_type=MOCK_OBJECT_TYPE,
            object_key=f"obs://{MOCK_BUCKET_NAME}/{MOCK_OBJECT_KEY}",
            metadata=MOCK_METADATA,
        )

        operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.create_object.assert_called_once_with(
            bucket_name=None,
            object_key=f"obs://{MOCK_BUCKET_NAME}/{MOCK_OBJECT_KEY}",
            object_type=MOCK_OBJECT_TYPE,
            data=MOCK_DATA,
            metadata=MOCK_METADATA,
            headers=self.headers,
        )


class TestOBSGetObjectOperator(unittest.TestCase):
    def setUp(self):
        self.operator = OBSGetObjectOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_OBS_CONN_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET_NAME,
            object_key=MOCK_OBJECT_KEY,
            download_path=MOCK_DOWNLOAD_PATH,
        )

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.OBSHook")
    def test_execute(self, mock_hook):
        self.operator.download_path = MOCK_DOWNLOAD_PATH
        self.operator.execute(MOCK_CONTEXT)
        mock_hook.return_value.get_object.return_value = None

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.get_object.assert_called_once_with(
            bucket_name=MOCK_BUCKET_NAME,
            object_key=MOCK_OBJECT_KEY,
            download_path=MOCK_DOWNLOAD_PATH,
        )
        self.assertIsNone(mock_hook.return_value.get_object.return_value)


class TestOBSDeleteObjectOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.OBSHook")
    def test_execute_if_bucket_exist(self, mock_hook):
        operator = OBSDeleteObjectOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_OBS_CONN_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET_NAME,
            object_key=MOCK_OBJECT_KEY,
        )
        operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.delete_object.assert_called_once_with(
            bucket_name=MOCK_BUCKET_NAME, object_key=MOCK_OBJECT_KEY, version_id=None
        )

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.OBSHook")
    def test_execute_if_not_bucket_exist(self, mock_hook):
        operator = OBSDeleteObjectOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_OBS_CONN_ID,
            region=MOCK_REGION,
            object_key=f"obs://{MOCK_BUCKET_NAME}/{MOCK_OBJECT_KEY}",
        )

        operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.delete_object.assert_called_once_with(
            bucket_name=None, object_key=f"obs://{MOCK_BUCKET_NAME}/{MOCK_OBJECT_KEY}", version_id=None
        )


class TestOBSDeleteBatchObjectOperator(unittest.TestCase):
    def setUp(self):
        self.object_list = ["mock_object_key1", "mock_object_key2"]
        self.operator = OBSDeleteBatchObjectOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_OBS_CONN_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET_NAME,
            object_list=self.object_list,
        )

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.OBSHook")
    def test_execute_if_bucket_exist(self, mock_hook):
        mock_hook.return_value.exist_bucket.return_value = True

        self.operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.exist_bucket.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_hook.return_value.delete_objects.assert_called_once_with(
            bucket_name=MOCK_BUCKET_NAME, object_list=self.object_list, quiet=True
        )

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.OBSHook")
    def test_execute_if_not_bucket_exist(self, mock_hook):
        mock_hook.return_value.exist_bucket.return_value = False
        mock_hook.return_value.region = MOCK_REGION

        self.operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.exist_bucket.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_hook.return_value.delete_objects.assert_not_called()


class TestOBSCopyObjectOperator(unittest.TestCase):
    def setUp(self):
        self.source_object_key = "source_object_key"
        self.dest_object_key = "dest_object_key"
        self.source_bucket_name = "source_bucket_name"
        self.dest_bucket_name = "dest_bucket_name"
        self.operator = OBSCopyObjectOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_OBS_CONN_ID,
            region=MOCK_REGION,
            source_object_key=self.source_object_key,
            dest_object_key=self.dest_object_key,
            source_bucket_name=self.source_bucket_name,
            dest_bucket_name=self.dest_bucket_name,
            metadata=MOCK_METADATA,
        )

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.OBSHook")
    def test_execute(self, mock_hook):

        self.operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.copy_object.assert_called_once_with(
            source_object_key=self.source_object_key,
            dest_object_key=self.dest_object_key,
            source_bucket_name=self.source_bucket_name,
            dest_bucket_name=self.dest_bucket_name,
            metadata=MOCK_METADATA,
            version_id=None,
            headers={"directive": None, "acl": None},
        )


class TestOBSMoveObjectOperator(unittest.TestCase):
    def setUp(self):
        self.source_object_key = "source_object_key"
        self.dest_object_key = "dest_object_key"
        self.operator = OBSMoveObjectOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_OBS_CONN_ID,
            region=MOCK_REGION,
            source_object_key=self.source_object_key,
            dest_object_key=self.dest_object_key,
            dest_bucket_name=MOCK_BUCKET_NAME,
        )

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.OBSHook")
    def test_execute_if_bucket_exist(self, mock_hook):
        mock_hook.return_value.exist_bucket.return_value = True

        self.operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.exist_bucket.assert_called()
        mock_hook.return_value.move_object.assert_called_once_with(
            source_bucket_name=MOCK_BUCKET_NAME,
            dest_bucket_name=MOCK_BUCKET_NAME,
            source_object_key=self.source_object_key,
            dest_object_key=self.dest_object_key,
        )

    @mock.patch("airflow.providers.huawei.cloud.operators.huawei_obs.OBSHook")
    def test_execute_if_not_bucket_exist(self, mock_hook):
        mock_hook.return_value.exist_bucket.return_value = False
        mock_hook.return_value.region = MOCK_REGION

        self.operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(huaweicloud_conn_id=MOCK_OBS_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.exist_bucket.assert_called()
        mock_hook.return_value.move_object.assert_not_called()
