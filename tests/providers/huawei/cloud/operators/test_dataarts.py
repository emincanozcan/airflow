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

from airflow.providers.huawei.cloud.operators.dataarts import DataArtsDLFStartJobOperator

MOCK_TASK_ID = "test-dli-operator"
MOCK_REGION = "mock_region"
MOCK_CDM_CONN_ID = "mock_cdm_conn_default"
MOCK_PROJECT_ID = "mock_project_id"
MOCK_WORKSPACE = "workspace-id"
MOCK_JOB_NAME = "job-name"
MOCK_BODY = {"jobParams": [{"name": "param1", "value": "value1"}]}


class TestDataArtsDLFStartJobOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.dataarts.DataArtsHook")
    def test_execute(self, mock_hook):
        operator = DataArtsDLFStartJobOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_CDM_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            workspace=MOCK_WORKSPACE,
            job_name=MOCK_JOB_NAME,
            body=MOCK_BODY,
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_CDM_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID
        )
        mock_hook.return_value.dlf_start_job.assert_called_once_with(
            workspace=MOCK_WORKSPACE, job_name=MOCK_JOB_NAME, body=MOCK_BODY
        )
