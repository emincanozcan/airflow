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
"""This module contains Huawei Cloud SMN operators."""
from __future__ import annotations

from typing import TYPE_CHECKING
import json

from airflow.models import BaseOperator
from airflow.providers.huawei.cloud.hooks.smn import SMNHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SMNPublishMessageTemplateOperator(BaseOperator):
    """
    This operator is used to publish template messages to a topic

    :param project_id: Specifies the project ID.For details about how to obtain the project ID
    :param topic_urn: Specifies the resource identifier of the topic, which is unique. To obtain the resource identifier.
    :param tags: Specifies the dictionary consisting of variable parameters and values.
    :param template_name: Specifies the message template name
    :param region: Regions where the API is available
    :param subject: Specifies the message subject, which is used as the email subject when you publish email messages
    :param huaweicloud_conn_id: The Airflow connection used for SMN credentials.
    """

    def __init__(
        self,
        project_id: str,
        topic_urn: str,
        tags: dict,
        template_name: str,
        region: str | None = None,
        subject: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.topic_urn = topic_urn
        self.subject = subject
        self.tags = tags
        self.template_name = template_name
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):

        # Connection parameter and kwargs parameter from Airflow UI
        smn_hook = SMNHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        smn_hook.send_message(topic_urn=self.topic_urn,
                              project_id=self.project_id,
                              tags=self.tags,
                              template_name=self.template_name)


class SMNPublishTextMessageOperator(BaseOperator):
    """
    This operator is used to publish text messages to a topic

    :param project_id: Specifies the project ID.For details about how to obtain the project ID
    :param topic_urn: Specifies the resource identifier of the topic, which is unique. To obtain the resource identifier.
    :param message: Specifies the message content
    :param region: Regions where the API is available
    :param subject: Specifies the message subject, which is used as the email subject when you publish email messages
    :param huaweicloud_conn_id: The Airflow connection used for SMN credentials.
    """

    def __init__(
        self,
        project_id: str,
        topic_urn: str,
        message: str,
        region: str | None = None,
        subject: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.region = region
        self.project_id = project_id
        self.topic_urn = topic_urn
        self.subject = subject
        self.message = message
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):

        # Connection parameter and kwargs parameter from Airflow UI
        smn_hook = SMNHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        smn_hook.send_message(topic_urn=self.topic_urn,
                              project_id=self.project_id,
                              message=self.message)


class SMNPublishJsonMessageOperator(BaseOperator):
    """
    This operator is used to publish json messages to a topic

    :param project_id: Specifies the project ID.For details about how to obtain the project ID
    :param topic_urn: Specifies the resource identifier of the topic, which is unique. To obtain the resource identifier.
    :param message_structure: Specifies the message structure, which contains JSON strings
    :param region: Regions where the API is available
    :param subject: Specifies the message subject, which is used as the email subject when you publish email messages
    :param huaweicloud_conn_id: The Airflow connection used for SMN credentials.
    """

    # TODO: update message_structure param -> sms, email etc.

    def __init__(
        self,
        project_id: str,
        topic_urn: str,
        default: str,
        sms: str | None = None,
        email: str | None = None,
        http: str | None = None,
        https: str | None = None,
        functionstage: str | None = None,
        region: str | None = None,
        subject: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        

        self.region = region
        self.project_id = project_id
        self.topic_urn = topic_urn
        self.subject = subject
        msg = {"default": default, "sms": sms, "email": email, "http": http, "https": https, "functionstage": functionstage}
        self.message_structure = json.dumps({i:j for i,j in msg.items() if j != None})
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):
        # Connection parameter and kwargs parameter from Airflow UI
        smn_hook = SMNHook(
            huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)

        smn_hook.send_message(topic_urn=self.topic_urn,
                              project_id=self.project_id,
                              message_structure=self.message_structure)
