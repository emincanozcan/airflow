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


class SMNTestOperator(BaseOperator):
    """
    This operator creates an OSS bucket

    :param region: OSS region you want to create bucket
    :param bucket_name: This is bucket name you want to create
    :param smn_conn_id: The Airflow connection used for OSS credentials.
    """

    def __init__(
        self,
        region: str,
        bucket_name: str | None = None,
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        print("ARGUMENTS")
        print(kwargs)
        self.huaweicloud_conn_id = huaweicloud_conn_id
        self.region = region
        self.bucket_name = bucket_name

    def execute(self, context: Context):
        smn_hook = SMNHook(huaweicloud_conn_id=self.huaweicloud_conn_id, region=self.region)
        smn_hook.test()
