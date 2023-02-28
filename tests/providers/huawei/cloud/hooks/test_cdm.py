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


from airflow.providers.huawei.cloud.hooks.cdm import CDMHook
from tests.providers.huawei.cloud.utils.hw_mock import mock_huawei_cloud_default, default_mock_constants

CDM_STRING = "airflow.providers.huawei.cloud.hooks.cdm.{}"
JOB_NAME = "job-name"
CLUSTER_ID = "cluster-id"
JOBS = [] # NOTE: Huawei Cloud doesn't accept empty job list. But, we have mocked the SDK, so there is no problem for tests.

class TestCdmHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            CDM_STRING.format("CDMHook.__init__"),
            new=mock_huawei_cloud_default,
        ):
            self.hook = CDMHook()

    def test_get_cdm_client(self):
        client = self.hook.get_cdm_client()
        assert client.get_credentials().ak == default_mock_constants["AK"]
        assert client.get_credentials().sk == default_mock_constants["SK"]
        assert client.get_credentials().project_id == default_mock_constants["PROJECT_ID"]

    @mock.patch(CDM_STRING.format("CdmSdk.CdmClient.create_job"))
    def test_create_job(self, create_job):
        self.hook.create_job(CLUSTER_ID, JOBS)
        request = self.hook.create_job_request(CLUSTER_ID, JOBS)
        create_job.assert_called_once_with(request)

    @mock.patch(CDM_STRING.format("CdmSdk.CdmClient.create_and_start_random_cluster_job"))
    def test_create_and_execute_job(self, create_and_start_random_cluster_job):
        x_language = "tr_tr"
        clusters = ["cluster-id"]
        self.hook.create_and_execute_job(
            x_language, clusters, JOBS)
        request = self.hook.create_and_execute_job_request(
            x_language, clusters, JOBS)
        create_and_start_random_cluster_job.assert_called_once_with(request)

    @mock.patch(CDM_STRING.format("CdmSdk.CdmClient.start_job"))
    def test_start_job(self, start_job):
        self.hook.start_job(CLUSTER_ID, JOB_NAME)
        request = self.hook.start_job_request(CLUSTER_ID, JOB_NAME)
        start_job.assert_called_once_with(request)
    
    @mock.patch(CDM_STRING.format("CdmSdk.CdmClient.delete_job"))
    def test_delete_job(self, delete_job):
        self.hook.delete_job(CLUSTER_ID, JOB_NAME)
        request = self.hook.delete_job_request(CLUSTER_ID, JOB_NAME)
        delete_job.assert_called_once_with(request)
    
    @mock.patch(CDM_STRING.format("CdmSdk.CdmClient.stop_job"))
    def test_stop_job(self, stop_job):
        self.hook.stop_job(CLUSTER_ID, JOB_NAME)
        request = self.hook.stop_job_request(CLUSTER_ID, JOB_NAME)
        stop_job.assert_called_once_with(request)
    
    @mock.patch(CDM_STRING.format("CdmSdk.CdmClient.show_job_status"))
    def test_show_job_status(self, show_job_status):
        self.hook.show_job_status(CLUSTER_ID, JOB_NAME)
        request = self.hook.show_job_status_request(CLUSTER_ID, JOB_NAME)
        show_job_status.assert_called_once_with(request)