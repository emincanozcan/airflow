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

from airflow.providers.huawei.cloud.hooks.dws import DWSHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DWSClusterSensor(BaseSensorOperator):
    """
    Waits for a DWS cluster to reach a specific status.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:DWSClusterSensor`

    :param cluster_name: The name for the cluster being pinged.
    :param target_status: The cluster status desired.
    """

    template_fields: Sequence[str] = ("cluster_name", "target_status")

    def __init__(
        self,
        *,
        cluster_name: str,
        target_status: str = "AVAILABLE",
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.target_status = target_status
        self.huaweicloud_conn_id = huaweicloud_conn_id
        self.hook: DWSHook | None = None

    def poke(self, context: Context):
        self.log.info("Poking for status : %s\nfor cluster %s", self.target_status, self.cluster_name)
        return self.get_hook().get_cluster_status(self.cluster_name) == self.target_status

    def get_hook(self) -> DWSHook:
        """Create and return a DWSHook"""
        if self.hook:
            return self.hook

        self.hook = DWSHook(huaweicloud_conn_id=self.huaweicloud_conn_id)
        return self.hook


class DWSSnapshotSensor(BaseSensorOperator):
    """
    Waits for a DWS snapshot to reach a specific status.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:DWSClusterSensor`

    :param snapshot_name: The name for the snapshot being pinged.
    :param target_status: The snapshot status desired.
    """

    template_fields: Sequence[str] = ("snapshot_name", "target_status")

    def __init__(
        self,
        *,
        snapshot_name: str,
        target_status: str = "AVAILABLE",
        huaweicloud_conn_id: str = "huaweicloud_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.snapshot_name = snapshot_name
        self.target_status = target_status
        self.huaweicloud_conn_id = huaweicloud_conn_id
        self.hook: DWSHook | None = None

    def poke(self, context: Context):
        self.log.info("Poking for status : %s\nfor snapshot %s", self.target_status, self.snapshot_name)
        return self.get_hook().get_snapshot_status(self.snapshot_name) == self.target_status

    def get_hook(self) -> DWSHook:
        """Create and return a DWSHook"""
        if self.hook:
            return self.hook

        self.hook = DWSHook(huaweicloud_conn_id=self.huaweicloud_conn_id)
        return self.hook
