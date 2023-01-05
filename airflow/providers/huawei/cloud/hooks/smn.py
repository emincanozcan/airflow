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
from functools import wraps
from inspect import signature
import json
from typing import TYPE_CHECKING, Callable, TypeVar, cast, Any
from urllib.parse import urlsplit
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdksmn.v2.region.smn_region import SmnRegion
from huaweicloudsdkcore.exceptions import exceptions
#from huaweicloudsdksmn.v2 import *
import huaweicloudsdksmn.v2 as SmnSdk

if TYPE_CHECKING:
    from airflow.models.connection import Connection

class SMNHook(BaseHook):
    conn_name_attr = "huaweicloud_conn_id"
    default_conn_name = "huaweicloud_default"
    conn_type = "huaweicloud"
    hook_name = "SMN"

    def __init__(self,
                 project_id: str,
                 topic_urn: str,
                 tags: str | None = None,
                 template_name: str | None = None,
                 subject: str | None = None,
                 message_structure: str | None = None, 
                 message: str | None = None, 
                 huaweicloud_conn_id="huaweicloud_default", 
                 *args, 
                 **kwargs) -> None:
        self.huaweicloud_conn_id = huaweicloud_conn_id
        self.smn_conn = self.get_connection(self.huaweicloud_conn_id)
        self.project_id = project_id
        self.topic_urn = topic_urn
        self.tags = tags
        self.template_name = template_name
        self.subject = subject
        self.message_structure = message_structure
        self.message = message
        super().__init__(*args, **kwargs)

    def get_region(self) -> str:
        #dddd = self.get_connection(self.huaweicloud_conn_id).extra_dejson.get('region',None)
        region = self.smn_conn.extra_dejson.get('region', None)
        if region is None:
            raise Exception(f"No region is specified for connection")
        return region


    def publish_message(self):
        
        kwargs = dict()

        if self.message_structure:
            kwargs["message_structure"] = self.message_structure
        if self.template_name:
            kwargs["message_template_name"] = self.template_name
        if self.subject:
            kwargs["subject"] = self.subject
        if self.tags:
            kwargs['tags'] = self.tags
        if self.message:
            kwargs['message'] = self.message
        
        self.send_request(self.make_publish_app_message_request(kwargs))
            
    def send_request(self, request: SmnSdk.PublishMessageRequest) -> None :     
        try:
            response = self.get_smn_client().publish_message(request)
            print(response)
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.request_id)
            print(e.error_code)
            print(e.error_msg)
            # TODO: Raise an exception!
            
    def make_publish_app_message_request(self,body: dict) -> SmnSdk.PublishMessageRequest:
        request = SmnSdk.PublishMessageRequest()
        request.topic_urn = self.topic_urn
        request.body = SmnSdk.PublishMessageRequestBody(**body)
        return request
            
    def get_smn_client(self) -> SmnSdk.SmnClient:
        
        ak = self.smn_conn.login
        sk = self.smn_conn.password
        project_id = self.project_id 

        credentials = BasicCredentials(ak, sk, project_id) 
        
        return SmnSdk.SmnClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(SmnRegion.value_of(self.get_region())) \
            .build()

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom UI field behaviour for Huawei Cloud Connection."""
        return {
            "hidden_fields": ["host", "schema", "port"],
            "relabeling": {
                "login": "Huawei Cloud Access Key ID",
                "password": "Huawei Cloud Secret Access Key",
            },
            "placeholders": {
                "login": "AKIAIOSFODNN7EXAMPLE",
                "password": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "extra": json.dumps(
                    {
                        "region" : "ap-southeast-3"
                    },
                    indent=2,
                ),
            },
        }