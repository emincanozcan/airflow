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
# from huaweicloudsdksmn.v3 import *

if TYPE_CHECKING:
    from airflow.models.connection import Connection

T = TypeVar("T", bound=Callable)

class SMNHook(BaseHook):
    conn_name_attr = "huaweicloud_conn_id"
    default_conn_name = "huaweicloud_default"
    conn_type = "huaweicloud"
    hook_name = "SMN"

    def __init__(self, region: str | None = None, huaweicloud_conn_id="huaweicloud_default", *args, **kwargs) -> None:
        self.huaweicloud_conn_id = huaweicloud_conn_id
        self.smn_conn = self.get_connection(huaweicloud_conn_id)
        self.region=region
        super().__init__(*args, **kwargs)

    def test(self):
        print(f"SMN Hook test {self.region}")

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
                    },
                    indent=2,
                ),
            },
        }