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

from typing import Any
from airflow.exceptions import AirflowException
from airflow.providers.huawei.cloud.hooks.base_huawei_cloud import HuaweiBaseHook

from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdksmn.v2.region.smn_region import SmnRegion

import huaweicloudsdksmn.v2 as SmnSdk

class SMNHook(HuaweiBaseHook):
    """Interact with Huawei Cloud SMN, using the huaweicloudsdksmn library."""

    def send_message(self,
                        project_id: str,
                        topic_urn: str,
                        tags: str | None = None,
                        template_name: str | None = None,
                        subject: str | None = None,
                        message_structure: str | None = None,
                        message: str | None = None, ):
        """
        This function is used to publish messages to a topic

        :param project_id: Specifies the project ID.For details about how to obtain the project ID
        :param topic_urn: Specifies the resource identifier of the topic, which is unique. To obtain the resource identifier.
        :param tags: Specifies the dictionary consisting of variable parameters and values.
        :param template_name: Specifies the message template name
        :param subject: Specifies the message subject, which is used as the email subject when you publish email messages
        :param message_structure: Specifies the message structure, which contains JSON strings
        :param message: Specifies the message content
        """
        
        kwargs = dict()

        if message_structure:
            kwargs["message_structure"] = message_structure
        if template_name:
            kwargs["message_template_name"] = template_name
        if subject:
            kwargs["subject"] = subject
        if tags:
            kwargs['tags'] = tags
        if message:
            kwargs['message'] = message

        self._send_request(project_id, self._make_publish_app_message_request(
            topic_urn=topic_urn, body=kwargs))

    def _send_request(self, project_id, request: SmnSdk.PublishMessageRequest) -> None:
        try:
            self._get_smn_client(project_id).publish_message(request)
            self.log.info("The message is published successfully")
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when publishing: {e}")

    def _make_publish_app_message_request(self, topic_urn, body: dict) -> SmnSdk.PublishMessageRequest:
        request = SmnSdk.PublishMessageRequest()
        request.topic_urn = topic_urn
        request.body = SmnSdk.PublishMessageRequestBody(**body)
        return request

    def _get_smn_client(self, project_id) -> SmnSdk.SmnClient:

        ak = self.conn.login
        sk = self.conn.password

        credentials = BasicCredentials(ak, sk, project_id)

        return SmnSdk.SmnClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(SmnRegion.value_of(self.get_region())) \
            .build()
