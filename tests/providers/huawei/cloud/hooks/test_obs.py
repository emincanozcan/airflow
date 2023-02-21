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

import copy
import unittest
from unittest import mock

from airflow.exceptions import AirflowException
from airflow.providers.huawei.cloud.hooks.huawei_obs import OBSHook
from tests.providers.huawei.cloud.utils.obs_mock import mock_obs_hook_default_conn, MOCK_BUCKET_NAME

OBS_STRING = "airflow.providers.huawei.cloud.hooks.huawei_obs.{}"
MOCK_OBS_CONN_ID = "mock_obs_conn_id"
MOCK_OBJECT_KEY = "mock_object_key"
MOCK_OBJECT_KEYS = ["mock1_object_key1", "mock2_object_key2", "mock1_object_key3",
                    "mock2_object_key4", "mock2_object_key5", "mock1_object_key6"]
MOCK_CONTENT = "mock_content"
MOCK_FILE_PATH = "mock_file_path"
MOCK_TAG_INFO = {"mock_key1": "mock_value1", "mock_key2": "mock_value2"}
RESP_200 = mock.Mock(status=200, reason="OK", errorCode=None, errorMessage=None)
RESP_404 = mock.Mock(status=404, reason="Not Found", errorCode=None, errorMessage=None)
RESP_403 = mock.Mock(status=403, reason="Forbidden", errorCode="AccessDenied", errorMessage="Access Denied")


class TestOBSHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            OBS_STRING.format("OBSHook.__init__"),
            new=mock_obs_hook_default_conn,
        ):
            self.hook = OBSHook(huaweicloud_conn_id=MOCK_OBS_CONN_ID)

    def test_get_conn(self):
        assert self.hook.get_conn() is not None

    def test_parse_obs_url(self):
        parsed = self.hook.parse_obs_url(f"obs://{MOCK_BUCKET_NAME}/{MOCK_OBJECT_KEY}")
        self.assertTupleEqual((MOCK_BUCKET_NAME, MOCK_OBJECT_KEY), parsed)

    @mock.patch(OBS_STRING.format("OBSHook.parse_obs_url"))
    def test_get_obs_bucket_object_key_if_not_bucket_name(self, mock_parse_obs_url):
        object_key = f"obs://{MOCK_BUCKET_NAME}/{MOCK_OBJECT_KEY}"
        mock_parse_obs_url.return_value = (MOCK_BUCKET_NAME, MOCK_OBJECT_KEY)

        self.hook.get_obs_bucket_object_key(
            bucket_name=None,
            object_key=f"obs://{MOCK_BUCKET_NAME}/{MOCK_OBJECT_KEY}",
            bucket_param_name="bucket_name",
            object_key_param_name="object_key",
        )

        mock_parse_obs_url.assert_called_once_with(object_key)

    @mock.patch(OBS_STRING.format("urlsplit"))
    @mock.patch(OBS_STRING.format("OBSHook"))
    def test_get_obs_bucket_object_key_if_relative_object_key(self, mock_hook, mock_urlsplit):
        # mock_urlsplit.return_value = mock.Mock(scheme="obs", netloc=MOCK_BUCKET_NAME)
        mock_urlsplit.return_value = mock.Mock(scheme="", netloc="")

        bucket_name, object_key = self.hook.get_obs_bucket_object_key(
            bucket_name=MOCK_BUCKET_NAME,
            object_key=MOCK_OBJECT_KEY,
            bucket_param_name="bucket_name",
            object_key_param_name="object_key",
        )
        mock_hook.return_value.parse_obs_url.assert_not_called()
        self.assertEqual(MOCK_BUCKET_NAME, bucket_name)
        self.assertEqual(MOCK_OBJECT_KEY, object_key)

    @mock.patch(OBS_STRING.format("urlsplit"))
    @mock.patch(OBS_STRING.format("OBSHook"))
    def test_get_obs_bucket_object_key_if_not_relative_object_key(self, mock_hook, mock_urlsplit):
        mock_urlsplit.return_value = mock.Mock(scheme="obs", netloc=MOCK_BUCKET_NAME)

        with self.assertRaises(TypeError):
            self.hook.get_obs_bucket_object_key(
                bucket_name=MOCK_BUCKET_NAME,
                object_key=f"obs://{MOCK_BUCKET_NAME}/{MOCK_OBJECT_KEY}",
                bucket_param_name="bucket_name",
                object_key_param_name="object_key",
            )
            mock_hook.return_value.parse_obs_url.assert_not_called()

    @mock.patch(OBS_STRING.format("OBSHook.get_credential"))
    @mock.patch(OBS_STRING.format("ObsClient"))
    def test_get_obs_client(self, mock_obs_client, mock_get_credential):
        mock_get_credential.return_value = ('AK', 'SK')

        self.hook.get_obs_client()

        mock_get_credential.assert_called_once_with()
        mock_obs_client.assert_called_once_with(
            access_key_id='AK',
            secret_access_key='SK',
            server=f'https://obs.{self.hook.region}.myhuaweicloud.com'
        )

    @mock.patch(OBS_STRING.format("OBSHook.get_obs_client"))
    def test_get_bucket_client(self, mock_get_obs_client):
        self.hook.get_bucket_client(MOCK_BUCKET_NAME)

        mock_get_obs_client.assert_called_once_with()
        mock_get_obs_client.return_value.bucketClient.assert_called_once_with(MOCK_BUCKET_NAME)

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_create_bucket_if_status_lt_300(self, mock_bucket_client):
        mock_create_bucket = mock_bucket_client.return_value.createBucket
        mock_create_bucket.return_value = RESP_200

        self.hook.create_bucket(MOCK_BUCKET_NAME)

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_create_bucket.assert_called_once_with(location=self.hook.region)

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_create_bucket_if_status_ge_300(self, mock_bucket_client):
        mock_create_bucket = mock_bucket_client.return_value.createBucket
        mock_create_bucket.return_value = RESP_404

        with self.assertRaises(AirflowException):
            self.hook.create_bucket(MOCK_BUCKET_NAME)

            mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
            mock_create_bucket.assert_called_once_with(location=self.hook.region)

    @mock.patch(OBS_STRING.format("OBSHook.get_obs_client"))
    def test_list_bucket_if_status_lt_300(self, mock_obs_client):
        mock_list_bucket = mock_obs_client.return_value.listBuckets

        resp_bucket_list = copy.deepcopy(RESP_200)
        mock_list_bucket.return_value = resp_bucket_list
        expect_buckets = [
            {"name": "mock_bucket1",
             "region": "cn-south-1",
             "create_date": "2023/01/13 10:00:00",
             "bucket_type": "OBJECT",
             },
            {"name": "mock_bucket2",
             "region": "cn-south-1",
             "create_date": "2023/01/14 10:00:00",
             "bucket_type": "OBJECT",
             },
        ]

        bucket1 = mock.Mock()
        bucket2 = mock.Mock()
        bucket1.configure_mock(name="mock_bucket1", location="cn-south-1",
                               create_date="2023/01/13 10:00:00", bucket_type="OBJECT")
        bucket2.configure_mock(name="mock_bucket2", location="cn-south-1",
                               create_date="2023/01/14 10:00:00", bucket_type="OBJECT")
        resp_bucket_list.body = mock.Mock(buckets=[bucket1, bucket2])

        res = self.hook.list_bucket()

        mock_obs_client.assert_called_once_with()
        mock_list_bucket.assert_called_once_with()
        self.assertListEqual(expect_buckets, res)

    @mock.patch(OBS_STRING.format("OBSHook.get_obs_client"))
    def test_list_bucket_if_status_ge_300(self, mock_obs_client):
        mock_list_bucket = mock_obs_client.return_value.listBuckets
        mock_list_bucket.return_value = RESP_404

        with self.assertRaises(AirflowException):
            self.hook.list_bucket()

            mock_obs_client.assert_called_once_with()
            mock_list_bucket.assert_called_once_with()

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_exist_bucket_if_status_lt_300(self, mock_bucket_client):
        mock_head_bucket = mock_bucket_client.return_value.headBucket
        mock_head_bucket.return_value = RESP_200

        res = self.hook.exist_bucket(MOCK_BUCKET_NAME)
        self.assertEqual(True, res)

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_exist_bucket_if_status_eq_404(self, mock_bucket_client):
        mock_head_bucket = mock_bucket_client.return_value.headBucket
        mock_head_bucket.return_value = RESP_404

        res = self.hook.exist_bucket(MOCK_BUCKET_NAME)
        self.assertEqual(False, res)

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_exist_bucket_if_status_eq_403(self, mock_bucket_client):
        mock_head_bucket = mock_bucket_client.return_value.headBucket
        mock_head_bucket.return_value = RESP_403

        res = self.hook.exist_bucket(MOCK_BUCKET_NAME)
        self.assertEqual(False, res)

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_delete_bucket_if_status_ge_300(self, mock_bucket_client):
        mock_delete_bucket = mock_bucket_client.return_value.deleteBucket
        mock_delete_bucket.return_value = RESP_404

        with self.assertRaises(AirflowException):
            self.hook.delete_bucket(MOCK_BUCKET_NAME)

            mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
            mock_delete_bucket.assert_called_once_with()

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_delete_bucket_if_status_lt_300(self, mock_bucket_client):
        mock_delete_bucket = mock_bucket_client.return_value.deleteBucket
        mock_delete_bucket.return_value = RESP_200

        self.hook.delete_bucket(MOCK_BUCKET_NAME)

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_delete_bucket.assert_called_once_with()

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_get_bucket_tagging_if_status_ge_300(self, mock_bucket_client):
        mock_get_bucket_tagging = mock_bucket_client.return_value.getBucketTagging
        resp_no_tagging = copy.deepcopy(RESP_404)
        resp_no_tagging.errorCode = "NoSuchTagSet"
        resp_no_tagging.errorMessage = "The TagSet does not exist"
        mock_get_bucket_tagging.return_value = resp_no_tagging

        with self.assertRaises(AirflowException):
            self.hook.get_bucket_tagging(MOCK_BUCKET_NAME)

            mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
            mock_get_bucket_tagging.assert_called_once_with()

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_get_bucket_tagging_if_status_lt_300(self, mock_bucket_client):
        mock_get_bucket_tagging = mock_bucket_client.return_value.getBucketTagging
        tag1 = {'key': 'version', 'value': 'v2.0'}
        tag2 = {'key': 'name', 'value': 'airflow'}
        expect_tag_set = [{'tag': 'version', 'value': 'v2.0'}, {'tag': 'name', 'value': 'airflow'}]
        body = {'tagSet': [mock.Mock(**tag1), mock.Mock(**tag2)]}
        resp_tagging = copy.deepcopy(RESP_200)
        resp_tagging.body = mock.Mock(**body)
        mock_get_bucket_tagging.return_value = resp_tagging

        tag_set = self.hook.get_bucket_tagging(MOCK_BUCKET_NAME)

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_get_bucket_tagging.assert_called_once_with()
        self.assertListEqual(expect_tag_set, tag_set)

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_set_bucket_tagging_if_not_tag_info(self, mock_bucket_client):
        mock_set_bucket_tagging = mock_bucket_client.return_value.setBucketTagging

        self.hook.set_bucket_tagging(
            tag_info=None,
            bucket_name=MOCK_BUCKET_NAME,
        )

        mock_bucket_client.assert_not_called()
        mock_set_bucket_tagging.assert_not_called()

    @mock.patch(OBS_STRING.format("TagInfo"))
    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_set_bucket_tagging_if_status_ge_300(self, mock_bucket_client, MockTagInfo):
        mock_set_bucket_tagging = mock_bucket_client.return_value.setBucketTagging
        mock_set_bucket_tagging.return_value = RESP_404

        with self.assertRaises(AirflowException):
            self.hook.set_bucket_tagging(
                tag_info=MOCK_TAG_INFO,
                bucket_name=MOCK_BUCKET_NAME,
            )

            mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
            mock_set_bucket_tagging.assert_called_once_with(tagInfo=MockTagInfo(MOCK_TAG_INFO))

    @mock.patch(OBS_STRING.format("TagInfo"))
    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_set_bucket_tagging_if_status_lt_300(self, mock_bucket_client, MockTagInfo):
        mock_set_bucket_tagging = mock_bucket_client.return_value.setBucketTagging
        mock_set_bucket_tagging.return_value = RESP_200

        self.hook.set_bucket_tagging(
            tag_info=MOCK_TAG_INFO,
            bucket_name=MOCK_BUCKET_NAME,
        )

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_set_bucket_tagging.assert_called_once_with(tagInfo=MockTagInfo(MOCK_TAG_INFO))

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_delete_bucket_tagging_if_status_lt_300(self, mock_bucket_client):
        mock_delete_bucket_tagging = mock_bucket_client.return_value.deleteBucketTagging
        mock_delete_bucket_tagging.return_value = RESP_200

        self.hook.delete_bucket_tagging(MOCK_BUCKET_NAME)

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_delete_bucket_tagging.assert_called_once_with()

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_delete_bucket_tagging_if_status_ge_300(self, mock_bucket_client):
        mock_delete_bucket_tagging = mock_bucket_client.return_value.deleteBucketTagging
        mock_delete_bucket_tagging.return_value = RESP_404

        with self.assertRaises(AirflowException):
            self.hook.delete_bucket_tagging(MOCK_BUCKET_NAME)

            mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
            mock_delete_bucket_tagging.assert_called_once_with()

    @mock.patch(OBS_STRING.format("OBSHook.get_obs_bucket_object_key"))
    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_exist_object_if_provided_bucket(self, mock_bucket_client, mock_get_obs_bucket_object_key):
        mock_get_obs_bucket_object_key.return_value = (MOCK_BUCKET_NAME, MOCK_OBJECT_KEY)
        mock_list_object = mock_bucket_client.return_value.listObjects

        body = {'contents': [mock.Mock(key=MOCK_OBJECT_KEY)]}
        resp_object_list = copy.deepcopy(RESP_200)
        resp_object_list.body = mock.Mock(**body)
        mock_list_object.return_value = resp_object_list

        exist = self.hook.exist_object(MOCK_OBJECT_KEY, MOCK_BUCKET_NAME)

        mock_get_obs_bucket_object_key.assert_called_once_with(MOCK_BUCKET_NAME, MOCK_OBJECT_KEY,
                                                               "bucket_name", "object_key")
        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_list_object.assert_called_once_with(prefix=MOCK_OBJECT_KEY, max_keys=1)
        self.assertEqual(True, exist)

    @mock.patch(OBS_STRING.format("OBSHook.get_obs_bucket_object_key"))
    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_exist_object_if_not_provided_bucket(self, mock_bucket_client, mock_get_obs_bucket_object_key):
        object_key = f"obs://{MOCK_BUCKET_NAME}/{MOCK_OBJECT_KEY}"
        mock_get_obs_bucket_object_key.return_value = (MOCK_BUCKET_NAME, MOCK_OBJECT_KEY)
        mock_list_object = mock_bucket_client.return_value.listObjects

        body = {'contents': [mock.Mock(key=MOCK_OBJECT_KEY)]}
        resp_object_list = copy.deepcopy(RESP_200)
        resp_object_list.body = mock.Mock(**body)
        mock_list_object.return_value = resp_object_list

        exist = self.hook.exist_object(bucket_name=None, object_key=object_key)

        mock_get_obs_bucket_object_key.assert_called_once_with(None, object_key,
                                                               "bucket_name", "object_key")
        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_list_object.assert_called_once_with(prefix=MOCK_OBJECT_KEY, max_keys=1)
        self.assertEqual(True, exist)

    @mock.patch(OBS_STRING.format("OBSHook.get_obs_bucket_object_key"))
    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_exist_object_if_status_ge_300(self, mock_bucket_client, mock_get_obs_bucket_object_key):
        object_key = f"obs://{MOCK_BUCKET_NAME}/{MOCK_OBJECT_KEY}"
        mock_get_obs_bucket_object_key.return_value = (MOCK_BUCKET_NAME, MOCK_OBJECT_KEY)
        mock_list_object = mock_bucket_client.return_value.listObjects
        mock_list_object.return_value = RESP_404

        exist = self.hook.exist_object(bucket_name=None, object_key=object_key)

        mock_get_obs_bucket_object_key.assert_called_once_with(None, object_key,
                                                               "bucket_name", "object_key")
        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_list_object.assert_called_once_with(prefix=MOCK_OBJECT_KEY, max_keys=1)
        self.assertEqual(False, exist)

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_list_object_if_status_lt_300(self, mock_bucket_client):
        mock_list_object = mock_bucket_client.return_value.listObjects
        body = {
            'contents': [mock.Mock(key=key) for key in MOCK_OBJECT_KEYS]
        }
        resp_object_list = copy.deepcopy(RESP_200)
        resp_object_list.body = mock.Mock(**body)
        mock_list_object.return_value = resp_object_list

        object_list = self.hook.list_object(bucket_name=MOCK_BUCKET_NAME)

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_list_object.assert_called_once_with(prefix=None, marker=None, max_keys=None)
        self.assertListEqual(MOCK_OBJECT_KEYS, object_list)

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_list_object_if_status_ge_300(self, mock_bucket_client):
        mock_list_object = mock_bucket_client.return_value.listObjects
        mock_list_object.return_value = RESP_404

        object_list = self.hook.list_object(bucket_name=MOCK_BUCKET_NAME)

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_list_object.assert_called_once_with(prefix=None, marker=None, max_keys=None)
        self.assertIsNone(object_list)

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_list_object_if_is_truncated(self, mock_bucket_client):
        mock_list_object = mock_bucket_client.return_value.listObjects
        body = {
            'contents': [mock.Mock(key=key) for key in MOCK_OBJECT_KEYS[:4]],
            'max_keys': 4,
            'is_truncated': True,
            'next_marker': 'mock2_object_key5',
        }
        resp_object_list = copy.deepcopy(RESP_200)
        resp_object_list.body = mock.Mock(**body)

        body_is_truncated = {
            'contents': [mock.Mock(key=key) for key in MOCK_OBJECT_KEYS[4:]],
            'max_keys': 4,
            'is_truncated': False,
            'next_marker': 'None',
        }
        resp_object_list_is_truncated = copy.deepcopy(RESP_200)
        resp_object_list_is_truncated.body = mock.Mock(**body_is_truncated)

        mock_list_object.side_effect = [resp_object_list, resp_object_list_is_truncated]

        object_list = self.hook.list_object(bucket_name=MOCK_BUCKET_NAME, is_truncated=True)

        self.assertEqual(2, mock_list_object.call_count)
        self.assertListEqual([MOCK_OBJECT_KEYS[:4], MOCK_OBJECT_KEYS[4:]], object_list)

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_list_object_if_max_keys(self, mock_bucket_client):
        mock_list_object = mock_bucket_client.return_value.listObjects
        body = {
            'contents': [mock.Mock(key=key) for key in MOCK_OBJECT_KEYS[:2]]
        }
        resp_object_list = copy.deepcopy(RESP_200)
        resp_object_list.body = mock.Mock(**body)
        mock_list_object.return_value = resp_object_list

        object_list = self.hook.list_object(bucket_name=MOCK_BUCKET_NAME, max_keys=2)

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_list_object.assert_called_once_with(prefix=None, marker=None, max_keys=2)
        self.assertListEqual(MOCK_OBJECT_KEYS[:2], object_list)

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_list_object_if_max_keys_and_prefix(self, mock_bucket_client):
        mock_list_object = mock_bucket_client.return_value.listObjects

        expect_object_list = list()
        body = {"contents": list()}
        n = 0
        for key in MOCK_OBJECT_KEYS:
            if key.startswith("mock2") and n < 2:
                body["contents"].append(mock.Mock(key=key))
                expect_object_list.append(key)
                n += 1

        resp_object_list = copy.deepcopy(RESP_200)
        resp_object_list.body = mock.Mock(**body)
        mock_list_object.return_value = resp_object_list

        object_list = self.hook.list_object(bucket_name=MOCK_BUCKET_NAME, max_keys=2, prefix="mock2")

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_list_object.assert_called_once_with(prefix="mock2", marker=None, max_keys=2)
        self.assertListEqual(expect_object_list, object_list)

    def test_create_object_if_object_type_fail(self):
        with self.assertRaises(AirflowException) as e:
            self.hook.create_object(
                bucket_name=MOCK_BUCKET_NAME,
                object_key=MOCK_OBJECT_KEY,
                object_type='test',
                data='test data'
            )

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_create_object_if_object_type_content(self, mock_bucket_client):
        mock_create_object = mock_bucket_client.return_value.putContent
        data = mock.Mock()
        data.read.return_value = b'content data'
        data.close.return_value = None
        body = {
            'objectUrl':
                f'https://{MOCK_BUCKET_NAME}.obs.{self.hook.region}.myhuaweicloud.com/{MOCK_OBJECT_KEY}'
        }
        resp_object = copy.deepcopy(RESP_200)
        resp_object.body = mock.Mock(**body)
        mock_create_object.return_value = resp_object

        self.hook.create_object(
            bucket_name=MOCK_BUCKET_NAME,
            object_key=MOCK_OBJECT_KEY,
            object_type='content',
            data=data
        )

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_create_object.assert_called_once_with(
            objectKey=MOCK_OBJECT_KEY,
            content=data,
            metadata=None,
            headers={}
        )

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_create_object_if_object_type_file(self, mock_bucket_client):
        mock_create_object = mock_bucket_client.return_value.putFile
        data = '/tmp/mock_file'
        body = {
            'objectUrl':
                f'https://{MOCK_BUCKET_NAME}.obs.{self.hook.region}.myhuaweicloud.com/{MOCK_OBJECT_KEY}'
        }
        resp_object_list = copy.deepcopy(RESP_200)
        resp_object_list.body = mock.Mock(**body)
        mock_create_object.return_value = resp_object_list

        self.hook.create_object(
            bucket_name=MOCK_BUCKET_NAME,
            object_key=MOCK_OBJECT_KEY,
            object_type='file',
            data=data,
            headers={'encryption': 'kms'}
        )

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_create_object.assert_called_once_with(
            objectKey=MOCK_OBJECT_KEY,
            file_path=data,
            metadata=None,
            headers={'sseHeader': {'encryption': 'kms', 'key': None}}
        )

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_create_object_if_object_type_folder(self, mock_bucket_client):
        mock_create_object = mock_bucket_client.return_value.putFile
        data = '/tmp/mock_folder/'
        prefix_object_url = f'https://{MOCK_BUCKET_NAME}.obs.{self.hook.region}.myhuaweicloud.com'
        resp = [
            (f'{data}test', [
                (f'{data}test/a', {'status': 200, 'reason': 'OK',
                                   'body': {'objectUrl': f'{prefix_object_url}/{data}test/a'}
                                   }),
                (f'{data}test/b', {'status': 200, 'reason': 'OK',
                                   'body': {'objectUrl': f'{prefix_object_url}/{data}test/b'},
                                   }),
                (f'{data}test/c', {'status': 200, 'reason': 'OK',
                                   'body': {
                                       'objectUrl': f'{prefix_object_url}/{data}test/c'},
                                   })
            ]),
            (f'{data}mock1.py',
             {'status': 400, 'reason': 'Bad Request', 'errorCode': 'EntityTooLarge',
              'errorMessage': 'Your proposed upload exceeds the maximum allowed object size.'}),
            (f'{data}mock2.py',
             {'status': 200, 'reason': 'OK', 'body': {'objectUrl': f'{prefix_object_url}/{data}mock2.py'},
              }),
            (f'{data}mock3.py',
             {'status': 200, 'reason': 'OK', 'body': {'objectUrl': f'{prefix_object_url}/{data}mock3.py'},
              })
        ]
        mock_create_object.return_value = resp

        expect_err_object = self.hook.create_object(
            bucket_name=MOCK_BUCKET_NAME,
            object_key=MOCK_OBJECT_KEY,
            object_type='file',
            data=data
        )

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_create_object.assert_called_once_with(
            objectKey=MOCK_OBJECT_KEY,
            file_path=data,
            metadata=None,
            headers={}
        )
        self.assertListEqual(['/tmp/mock_folder/mock1.py'], expect_err_object)

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_get_object(self, mock_bucket_client):
        mock_get_object = mock_bucket_client.return_value.getObject
        resp_object = copy.deepcopy(RESP_200)
        body = {
            'buffer': b'test get object\nxxx xxxx \nxxx xxxx'
        }
        resp_object.body = mock.Mock(**body)
        mock_get_object.return_value = resp_object

        self.hook.get_object(
            object_key=MOCK_OBJECT_KEY,
            bucket_name=MOCK_BUCKET_NAME,
            download_path='/mock/download/test'
        )

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_get_object.assert_called_once_with(
            objectKey=MOCK_OBJECT_KEY,
            downloadPath='/mock/download/test',
            loadStreamInMemory=False
        )

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_get_object_if_load_stream_in_memory(self, mock_bucket_client):
        mock_get_object = mock_bucket_client.return_value.getObject
        resp_object = copy.deepcopy(RESP_200)
        body = {
            'buffer': b'test get object\nxxx xxxx \nxxx xxxx'
        }
        resp_object.body = mock.Mock(**body)
        mock_get_object.return_value = resp_object

        self.hook.get_object(
            object_key=MOCK_OBJECT_KEY,
            bucket_name=MOCK_BUCKET_NAME,
            load_stream_in_memory=True
        )

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_get_object.assert_called_once_with(
            objectKey=MOCK_OBJECT_KEY,
            downloadPath=None,
            loadStreamInMemory=True
        )

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_get_object_if_status_ge_300(self, mock_bucket_client):
        mock_get_object = mock_bucket_client.return_value.getObject
        resp_object = copy.deepcopy(RESP_404)
        resp_object.errorCode = 'NoSuchKey'
        resp_object.errorMessage = 'The specified key does not exist.'
        mock_get_object.return_value = resp_object

        with self.assertRaises(AirflowException):
            self.hook.get_object(
                object_key=MOCK_OBJECT_KEY,
                bucket_name=MOCK_BUCKET_NAME,
                load_stream_in_memory=True
            )

            mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
            mock_get_object.assert_called_once_with(
                objectKey=MOCK_OBJECT_KEY,
                downloadPath=None,
                loadStreamInMemory=True
            )

    @mock.patch(OBS_STRING.format("OBSHook.get_obs_client"))
    def test_copy_object_if_status_lt_300(self, mock_obs_client):
        mock_copy_object = mock_obs_client.return_value.copyObject
        mock_copy_object.return_value = RESP_200

        self.hook.copy_object(
            source_object_key=MOCK_OBJECT_KEY,
            dest_object_key='mock_dest_object_key',
            source_bucket_name=MOCK_BUCKET_NAME,
            dest_bucket_name=MOCK_BUCKET_NAME,
        )

        mock_obs_client.assert_called_once_with()
        mock_copy_object.assert_called_once_with(
            sourceObjectKey=MOCK_OBJECT_KEY,
            destObjectKey='mock_dest_object_key',
            sourceBucketName=MOCK_BUCKET_NAME,
            destBucketName=MOCK_BUCKET_NAME,
            versionId=None,
            headers=None,
            metadata=None,
        )

    @mock.patch(OBS_STRING.format("OBSHook.get_obs_client"))
    def test_copy_object_if_status_ge_300(self, mock_obs_client):
        mock_copy_object = mock_obs_client.return_value.copyObject
        mock_copy_object.return_value = RESP_404

        with self.assertRaises(AirflowException):
            self.hook.copy_object(
                source_object_key=MOCK_OBJECT_KEY,
                dest_object_key='mock_dest_object_key',
                source_bucket_name=MOCK_BUCKET_NAME,
                dest_bucket_name=MOCK_BUCKET_NAME,
            )

            mock_obs_client.assert_called_once_with()
            mock_copy_object.assert_called_once_with(
                sourceObjectKey=MOCK_OBJECT_KEY,
                destObjectKey='mock_dest_object_key',
                sourceBucketName=MOCK_BUCKET_NAME,
                destBucketName=MOCK_BUCKET_NAME,
                versionId=None,
                headers=None,
                metadata=None,
            )

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_delete_object_if_status_lt_300(self, mock_bucket_client):
        mock_delete_object = mock_bucket_client.return_value.deleteObject
        mock_delete_object.return_value = RESP_200

        self.hook.delete_object(
            object_key=MOCK_OBJECT_KEY,
            bucket_name=MOCK_BUCKET_NAME,
        )

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_delete_object.assert_called_once_with(
            objectKey=MOCK_OBJECT_KEY,
            versionId=None
        )

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_delete_object_if_status_ge_300(self, mock_bucket_client):
        mock_delete_object = mock_bucket_client.return_value.deleteObject
        mock_delete_object.return_value = RESP_404

        with self.assertRaises(AirflowException):
            self.hook.delete_object(
                object_key=MOCK_OBJECT_KEY,
                bucket_name=MOCK_BUCKET_NAME,
            )

            mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
            mock_delete_object.assert_called_once_with(
                objectKey=MOCK_OBJECT_KEY,
                versionId=None
            )

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_delete_objects_if_not_object_list(self, mock_bucket_client):
        mock_delete_objects = mock_bucket_client.return_value.deleteObjects

        self.hook.delete_objects(
            object_list=[],
            bucket_name=MOCK_BUCKET_NAME,
        )

        mock_bucket_client.not_called()
        mock_delete_objects.not_called()

    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_delete_objects_if_object_list_gt_1000(self, mock_bucket_client):
        mock_delete_objects = mock_bucket_client.return_value.deleteObjects
        mock_delete_objects.return_value = RESP_404

        self.hook.delete_objects(
            object_list=['i' for i in range(1001)],
            bucket_name=MOCK_BUCKET_NAME,
        )

        mock_bucket_client.not_called()
        mock_delete_objects.not_called()

    @mock.patch(OBS_STRING.format("DeleteObjectsRequest"))
    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_delete_objects_if_status_lt_300(self, mock_bucket_client, mock_delete_objects_request):
        mock_delete_objects = mock_bucket_client.return_value.deleteObjects
        mock_delete_objects.return_value = RESP_200
        mock_delete_objects_request.return_value = {'quiet': True,
                                                    'objects': [{'key': 'mock1_object_key1'},
                                                                {'key': 'mock2_object_key2'},
                                                                {'key': 'mock1_object_key3'},
                                                                {'key': 'mock2_object_key4'},
                                                                {'key': 'mock2_object_key5'},
                                                                {'key': 'mock1_object_key6'}]}
        self.hook.delete_objects(
            object_list=MOCK_OBJECT_KEYS,
            bucket_name=MOCK_BUCKET_NAME,
        )

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_delete_objects.assert_called_once_with(mock_delete_objects_request.return_value)

    @mock.patch(OBS_STRING.format("DeleteObjectsRequest"))
    @mock.patch(OBS_STRING.format("OBSHook.get_bucket_client"))
    def test_delete_objects_if_object_list_contains_dict(self, mock_bucket_client, mock_delete_objects_request):
        mock_delete_objects = mock_bucket_client.return_value.deleteObjects
        mock_delete_objects.return_value = RESP_200
        mock_delete_objects_request.return_value = {'quiet': True,
                                                    'objects': [{'key': 'mock_key1', 'versionId': 'v1'},
                                                                {'key': 'mock_key2', 'versionId': 'v2'},
                                                                {'key': 'mock_key3'}]}
        self.hook.delete_objects(
            object_list=[
                {'object_key': 'mock_key1', 'version_id': 'v1'},
                {'object_key': 'mock_key2', 'version_id': 'v2'},
                {'object_key': 'mock_key3'},
            ],
            bucket_name=MOCK_BUCKET_NAME,
        )

        mock_bucket_client.assert_called_once_with(MOCK_BUCKET_NAME)
        mock_delete_objects.assert_called_once_with(mock_delete_objects_request.return_value)

    @mock.patch(OBS_STRING.format("OBSHook.get_obs_client"))
    def test_move_object(self, mock_obs_client):
        mock_copy_object = mock_obs_client.return_value.copyObject
        mock_delete_object = mock_obs_client.return_value.deleteObject
        mock_copy_object.return_value = RESP_200
        mock_delete_object.return_value = RESP_200

        self.hook.move_object(
            source_object_key=MOCK_OBJECT_KEY,
            dest_object_key='dest_mock_object_key',
            source_bucket_name=MOCK_BUCKET_NAME,
            dest_bucket_name=MOCK_BUCKET_NAME,
        )

        mock_obs_client.assert_called_once_with()
        mock_copy_object.assert_called_once_with(
            sourceObjectKey=MOCK_OBJECT_KEY,
            destObjectKey='dest_mock_object_key',
            sourceBucketName=MOCK_BUCKET_NAME,
            destBucketName=MOCK_BUCKET_NAME,
        )
        mock_delete_object.assert_called()

    @mock.patch(OBS_STRING.format("OBSHook.get_obs_client"))
    def test_move_object_if_copy_status_ge_300(self, mock_obs_client):
        mock_copy_object = mock_obs_client.return_value.copyObject
        mock_delete_object = mock_obs_client.return_value.deleteObject
        mock_copy_object.return_value = RESP_404

        with self.assertRaises(AirflowException):
            self.hook.move_object(
                source_object_key=MOCK_OBJECT_KEY,
                dest_object_key='dest_mock_object_key',
                source_bucket_name=MOCK_BUCKET_NAME,
                dest_bucket_name=MOCK_BUCKET_NAME,
            )

            mock_obs_client.assert_called_once_with()
            mock_copy_object.assert_called_once_with(
                sourceObjectKey=MOCK_OBJECT_KEY,
                destObjectKey='dest_mock_object_key',
                sourceBucketName=MOCK_BUCKET_NAME,
                destBucketName=MOCK_BUCKET_NAME
            )
            mock_delete_object.not_called()

    @mock.patch(OBS_STRING.format("OBSHook.get_obs_client"))
    def test_move_object_if_delete_status_ge_300(self, mock_obs_client):
        mock_copy_object = mock_obs_client.return_value.copyObject
        mock_delete_object = mock_obs_client.return_value.deleteObject
        mock_copy_object.return_value = RESP_200
        mock_delete_object.return_value = RESP_404

        with self.assertRaises(AirflowException):
            self.hook.move_object(
                source_object_key=MOCK_OBJECT_KEY,
                dest_object_key='dest_mock_object_key',
                source_bucket_name=MOCK_BUCKET_NAME,
                dest_bucket_name=MOCK_BUCKET_NAME,
            )

            mock_obs_client.assert_called_once_with()
            mock_copy_object.assert_called_once_with(
                sourceObjectKey=MOCK_OBJECT_KEY,
                destObjectKey='dest_mock_object_key',
                destBucketName=MOCK_BUCKET_NAME,
                sourceBucketName=MOCK_BUCKET_NAME,
            )
            mock_delete_object.assert_called()

    def test_get_credential(self):
        self.assertTupleEqual(('AK', 'SK'), self.hook.get_credential())

    def test_get_default_region(self):
        self.assertEqual("cn-south-1", self.hook.get_default_region())
