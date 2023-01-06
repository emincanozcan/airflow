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

from airflow.models import BaseOperator
from airflow.providers.huawei.cloud.hooks.smn import SMNHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SMNPublishMessageTemplateOperator(BaseOperator):
    def __init__(
        self,
        project_id: str | None = None,
        topic_urn: str | None = None,
        subject: str | None = None,
        tags: dict | None = None,
        template_name: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
        ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.topic_urn = topic_urn
        self.subject = subject
        self.tags = tags
        self.template_name = template_name
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):
        
        # Connection parameter and kwargs parameter from Airflow UI
        smn_hook = SMNHook(huaweicloud_conn_id = self.huaweicloud_conn_id, 
                           topic_urn = self.topic_urn,
                           project_id = self.project_id,
                           tags = self.tags,
                           template_name = self.template_name)
                           
        smn_hook.send_message()

class SMNPublishTextMessageOperator(BaseOperator):
    def __init__(
        self,
        project_id: str | None = None,
        topic_urn: str | None = None,
        subject: str | None = None,
        message: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
        ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.topic_urn = topic_urn
        self.subject = subject
        self.message = message
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):
        
        # Connection parameter and kwargs parameter from Airflow UI
        smn_hook = SMNHook(huaweicloud_conn_id = self.huaweicloud_conn_id, 
                           topic_urn = self.topic_urn,
                           project_id = self.project_id,
                           message = self.message)
                           
        smn_hook.send_message()

class SMNPublishJsonMessageOperator(BaseOperator):
    
    #TODO: update message_structure param -> sms, email etc.

    def __init__(
        self,
        project_id: str | None = None,
        topic_urn: str | None = None,
        subject: str | None = None,
        message_structure: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
        ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.topic_urn = topic_urn
        self.subject = subject
        self.message_structure = message_structure
        self.huaweicloud_conn_id = huaweicloud_conn_id

    def execute(self, context: Context):
        
        # Connection parameter and kwargs parameter from Airflow UI
        smn_hook = SMNHook(huaweicloud_conn_id = self.huaweicloud_conn_id, 
                           topic_urn = self.topic_urn,
                           project_id = self.project_id,
                           message_structure = self.message_structure)
                           
        smn_hook.send_message()