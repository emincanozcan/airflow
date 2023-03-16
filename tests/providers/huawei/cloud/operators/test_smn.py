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

from airflow.providers.huawei.cloud.operators.smn import (
    SMNPublishTextMessageOperator,
    SMNPublishJsonMessageOperator,
    SMNPublishMessageTemplateOperator
)

MOCK_TASK_ID = "test-smn-operator"
MOCK_REGION = "mock_region"
MOCK_SMN_CONN_ID = "mock_smn_conn_default"
MOCK_PROJECT_ID = "mock_project_id"
MOCK_TOPIC_URN = "mock_topic_urn"
MOCK_MESSAGE = "mock_message"
MOCK_TEMPLATE_NAME = "mock_template_name"
MOCK_TAGS = {"test_param": "test"}
MOCK_DEFAULT_MESSAGE = "Hello"
MOCK_SMS_MESSAGE = "Sms Message"
MOCK_SUBJECT = "mock_subject"


class TestSMNPublishTextMessageOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.smn.SMNHook")
    def test_execute(self, mock_hook):
        operator = SMNPublishTextMessageOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_SMN_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            topic_urn=MOCK_TOPIC_URN,
            message=MOCK_MESSAGE,
            subject=MOCK_SUBJECT
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_SMN_CONN_ID,
            region=MOCK_REGION,
            project_id=MOCK_PROJECT_ID
        )
        mock_hook.return_value.send_message.assert_called_once_with(
            topic_urn=MOCK_TOPIC_URN,
            message=MOCK_MESSAGE,
            subject=MOCK_SUBJECT
        )


class TestSMNPublishJsonMessageOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.smn.SMNHook")
    def test_execute(self, mock_hook):
        operator = SMNPublishJsonMessageOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_SMN_CONN_ID,
            subject=MOCK_SUBJECT,
            project_id=MOCK_PROJECT_ID,
            topic_urn=MOCK_TOPIC_URN,
            default=MOCK_DEFAULT_MESSAGE,
            sms=MOCK_SMS_MESSAGE
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_SMN_CONN_ID,
            region=MOCK_REGION,
            project_id=MOCK_PROJECT_ID
        )
        mock_hook.return_value.send_message.assert_called_once_with(
            topic_urn=MOCK_TOPIC_URN,
            message_structure=json.dumps(
                {"default": MOCK_DEFAULT_MESSAGE, "sms": MOCK_SMS_MESSAGE}),
            subject=MOCK_SUBJECT
        )


class TestSMNPublishMessageTemplateOperator(unittest.TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.smn.SMNHook")
    def test_execute(self, mock_hook):
        operator = SMNPublishMessageTemplateOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            huaweicloud_conn_id=MOCK_SMN_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            topic_urn=MOCK_TOPIC_URN,
            tags=MOCK_TAGS,
            template_name=MOCK_TEMPLATE_NAME,
            subject=MOCK_SUBJECT
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_SMN_CONN_ID,
            region=MOCK_REGION,
            project_id=MOCK_PROJECT_ID
        )
        mock_hook.return_value.send_message.assert_called_once_with(
            topic_urn=MOCK_TOPIC_URN,
            tags=MOCK_TAGS,
            template_name=MOCK_TEMPLATE_NAME,
            subject=MOCK_SUBJECT)
