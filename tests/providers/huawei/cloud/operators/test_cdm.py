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

from airflow.providers.huawei.cloud.operators.cdm import (
    CDMCreateJobOperator,
    CDMCreateAndExecuteJobOperator,
    CDMStartJobOperator,
    CDMStopJobOperator,
    CDMDeleteJobOperator
)

MOCK_TASK_ID = "test-dli-operator"
MOCK_REGION = "mock_region"
MOCK_CDM_CONN_ID = "mock_cdm_conn_default"
MOCK_PROJECT_ID = "mock_project_id"
MOCK_CLUSTER_ID = "cluster-id"
MOCK_CLUSTERS = ["cluster-id"]
MOCK_X_LANGUAGE = "tr_tr"
MOCK_JOB_NAME = "job-name"
MOCK_JOBS = []

class TestCDMCreateJobOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.cdm.CDMHook")
    def test_execute(self, mock_hook):
        operator = CDMCreateJobOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_CDM_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            cluster_id=MOCK_CLUSTER_ID,
            jobs=MOCK_JOBS
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_CDM_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID)
        mock_hook.return_value.create_job.assert_called_once_with(
            cluster_id=MOCK_CLUSTER_ID,
            jobs=MOCK_JOBS
        )

class TestCDMCreateAndExecuteJobOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.cdm.CDMHook")
    def test_execute(self, mock_hook):
        operator = CDMCreateAndExecuteJobOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_CDM_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            jobs=MOCK_JOBS,
            clusters=MOCK_CLUSTERS,
            x_language=MOCK_X_LANGUAGE
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_CDM_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID)
        mock_hook.return_value.create_and_execute_job.assert_called_once_with(
            jobs=MOCK_JOBS,
            clusters=MOCK_CLUSTERS,
            x_language=MOCK_X_LANGUAGE
        )

class TestCDMStartJobOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.cdm.CDMHook")
    def test_execute(self, mock_hook):
        operator = CDMStartJobOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_CDM_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            cluster_id=MOCK_CLUSTER_ID,
            job_name=MOCK_JOB_NAME
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_CDM_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID)
        mock_hook.return_value.start_job.assert_called_once_with(
            cluster_id=MOCK_CLUSTER_ID,
            job_name=MOCK_JOB_NAME
        )

class TestCDMStopJobOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.cdm.CDMHook")
    def test_execute(self, mock_hook):
        operator = CDMStopJobOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_CDM_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            cluster_id=MOCK_CLUSTER_ID,
            job_name=MOCK_JOB_NAME
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_CDM_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID)
        mock_hook.return_value.stop_job.assert_called_once_with(
            cluster_id=MOCK_CLUSTER_ID,
            job_name=MOCK_JOB_NAME
        )

class TestCDMDeleteJobOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.cdm.CDMHook")
    def test_execute(self, mock_hook):
        operator = CDMDeleteJobOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_CDM_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            cluster_id=MOCK_CLUSTER_ID,
            job_name=MOCK_JOB_NAME
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_CDM_CONN_ID, region=MOCK_REGION, project_id=MOCK_PROJECT_ID)
        mock_hook.return_value.delete_job.assert_called_once_with(
            cluster_id=MOCK_CLUSTER_ID,
            job_name=MOCK_JOB_NAME
        )