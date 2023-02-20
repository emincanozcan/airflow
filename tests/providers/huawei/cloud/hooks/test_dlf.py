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
import json

import unittest
from unittest import mock


from airflow.providers.huawei.cloud.hooks.dlf import DLFHook
from tests.providers.huawei.cloud.utils.hw_mock import mock_huawei_cloud_default

DLF_STRING = "airflow.providers.huawei.cloud.hooks.dlf.{}"
MOCK_DLF_CONN_ID = "mock_dlf_default"
PROJECT_ID = "project-id"
JOB_NAME = "job-name"
WORKSPACE = "workspace-id"
BODY = {"jobParams": [{"name": "param1", "value": "value1"}]}

class TestDlfHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            DLF_STRING.format("DLFHook.__init__"),
            new=mock_huawei_cloud_default,
        ):
            self.hook = DLFHook(huaweicloud_conn_id=MOCK_DLF_CONN_ID)

    def test_get_default_region(self):
        assert self.hook.get_region() == "ap-southeast-3"

    def test_get_dlf_client(self):
        client = self.hook._get_dlf_client(PROJECT_ID)
        assert client.get_credentials().ak == "AK"
        assert client.get_credentials().sk == "SK"
        assert client.get_credentials().project_id == PROJECT_ID

    @mock.patch(DLF_STRING.format("DlfSdk.DlfClient.start_job"))
    def test_start_job(self, start_job):
        self.hook.start_job(PROJECT_ID, WORKSPACE, JOB_NAME, BODY)
        request = self.hook.start_job_request(WORKSPACE, JOB_NAME, BODY)
        start_job.assert_called_once_with(request)
        
    @mock.patch(DLF_STRING.format("DlfSdk.DlfClient.show_job_status"))
    def test_show_job_status(self, show_job_status):
        self.hook.show_job_status(PROJECT_ID, WORKSPACE, JOB_NAME)
        request = self.hook.show_job_status_request(WORKSPACE, JOB_NAME)
        show_job_status.assert_called_once_with(request)