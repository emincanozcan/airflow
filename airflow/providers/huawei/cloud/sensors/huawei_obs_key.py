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

from typing import TYPE_CHECKING, Sequence
from urllib.parse import urlsplit

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.providers.huawei.cloud.hooks.huawei_obs import OBSHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OBSObjectKeySensor(BaseSensorOperator):
    """
    Waits for a object key (a file-like instance on OBS) to be present in a OBS bucket.

    :param obs_conn_id: The Airflow connection used for OBS credentials.
    :param region: OBS region
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可判断任意区域的桶中对象
    :param object_key: The key being waited on. Supports full obs:// style url
        or relative path from root level. When it's specified as a full obs://
        url, please leave bucket_name as `None`.
    :param bucket_name: OBS bucket name
    """

    template_fields: Sequence[str] = ("object_key", "bucket_name")

    def __init__(
        self,
        object_key: str | list[str],
        region: str | None = None,
        bucket_name: str | None = None,
        obs_conn_id: str | None = "obs_default",
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.obs_conn_id = obs_conn_id
        self.region = region
        self.bucket_name = bucket_name
        self.object_key = [object_key] if isinstance(object_key, str) else object_key
        self.hook: OBSHook | None = None
        self.object_list = None
    
    def _check_object_key(self, object_key):
        bucket_name, object_key = OBSHook.get_obs_bucket_object_key(
            self.bucket_name, object_key, "bucket_name", "object_key"
        )
        res = self.get_hook().exist_object(object_key=object_key, bucket_name=bucket_name)
        return res

    def poke(self, context: Context):
        """
        Check if the object exists in the bucket to pull key.
        :param self - the object itself
        :param context - the context of the object
        :returns True if the object exists, False otherwise
        """
        return all(self._check_object_key(object_key) for object_key in self.object_key)

    def get_hook(self) -> OBSHook:
        """Create and return an OBSHook"""
        if self.hook:
            return self.hook

        self.hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)
        return self.hook
