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

from datetime import datetime

from airflow import DAG

from airflow.providers.huawei.cloud.sensors.dws import DWSClusterSensor, DWSSnapshotSensor
from airflow.providers.huawei.cloud.operators.dws import (
    DWSCreateClusterOperator,
    DWSCreateClusterSnapshotOperator,
    DWSDeleteClusterSnapshotOperator,
    DWSRestoreClusterOperator,
    DWSDeleteClusterOperator,
    DWSDeleteClusterBasedOnSnapshotOperator,
)


DAG_ID = "example_dws"
DB_USER = "dbadmin"
DB_PWD = "AdminPassword"


with DAG(
    dag_id=DAG_ID,
    default_args={
        "huaweicloud_conn_id": 'huaweicloud_default'
    },
    schedule="@once",
    start_date=datetime(2023, 1, 29),
    catchup=False,
    tags=["example"],
) as dag:
    cluster_name = "example_cluster"
    snapshot_name = "example_snapshot"
    cluster_name_2 = "example_cluster_2"
    snapshot_name_2 = "example_snapshot_2"
    restore_cluster_name = f"restore_{cluster_name}"
    description = "test"
    subnet_id = "youself_subnet_id"
    security_group_id = "yourself_security_group_id"
    vpc_id = "yourself_vpc_id"
    availability_zone = "cn-south-2b"

    # [START howto_operator_dws_create_cluster]
    create_cluster = DWSCreateClusterOperator(
        task_id="create_cluster",
        name=cluster_name,
        node_type='dwsk2.xlarge',
        number_of_node=3,
        number_of_cn=3,
        subnet_id=subnet_id,
        security_group_id=security_group_id,
        vpc_id=vpc_id,
        availability_zone=availability_zone,
        user_name=DB_USER,
        user_pwd=DB_PWD,
        public_bind_type='auto_assign',
    )
    # [END howto_operator_dws_create_cluster]

    # [START howto_operator_dws_create_cluster]
    create_cluster_2 = DWSCreateClusterOperator(
        task_id="create_cluster_2",
        name=cluster_name_2,
        node_type='dwsk2.xlarge',
        number_of_node=3,
        number_of_cn=3,
        subnet_id=subnet_id,
        security_group_id=security_group_id,
        vpc_id=vpc_id,
        availability_zone=availability_zone,
        user_name=DB_USER,
        user_pwd=DB_PWD,
        public_bind_type='auto_assign',
    )
    # [END howto_operator_dws_create_cluster]

    # [START howto_sensor_dws_wait_cluster_available]
    wait_cluster_available = DWSClusterSensor(
        task_id="wait_cluster_available",
        cluster_name=cluster_name,
        target_status="AVAILABLE",
        poke_interval=15,
        timeout=60 * 20,
    )
    # [END howto_sensor_dws_wait_cluster_available]

    # [START howto_sensor_dws_wait_cluster_available]
    wait_cluster_available_2 = DWSClusterSensor(
        task_id="wait_cluster_available_2",
        cluster_name=cluster_name_2,
        target_status="AVAILABLE",
        poke_interval=15,
        timeout=60 * 20,
    )
    # [END howto_sensor_dws_wait_cluster_available]

    # [START howto_operator_dws_create_cluster_snapshot]
    create_cluster_snapshot = DWSCreateClusterSnapshotOperator(
        task_id="create_cluster_snapshot",
        name=snapshot_name,
        cluster_name=cluster_name,
        description=description,
    )
    # [END howto_operator_dws_create_cluster_snapshot]

    # [START howto_operator_dws_create_cluster_snapshot]
    create_cluster_snapshot_2 = DWSCreateClusterSnapshotOperator(
        task_id="create_cluster_snapshot_2",
        name=snapshot_name_2,
        cluster_name=cluster_name_2,
        description=description,
    )
    # [END howto_operator_dws_create_cluster_snapshot]

    # [START howto_sensor_dws_wait_snapshot_available]
    wait_snapshot_available = DWSSnapshotSensor(
        task_id="wait_snapshot_available",
        snapshot_name=snapshot_name,
        target_status="AVAILABLE",
        poke_interval=15,
        timeout=60 * 15,
    )
    # [END howto_sensor_dws_wait_snapshot_available]

    # [START howto_sensor_dws_wait_snapshot_available]
    wait_snapshot_available_2 = DWSSnapshotSensor(
        task_id="wait_snapshot_available_2",
        snapshot_name=snapshot_name_2,
        target_status="AVAILABLE",
        poke_interval=15,
        timeout=60 * 15,
    )
    # [END howto_sensor_dws_wait_snapshot_available]

    # [START howto_operator_dws_restore_cluster]
    restore_cluster = DWSRestoreClusterOperator(
        task_id="restore_cluster",
        name=restore_cluster_name,
        snapshot_name=snapshot_name,
    )
    # [END howto_operator_dws_restore_cluster]

    # [START howto_sensor_dws_wait_cluster_available]
    wait_restore_cluster_available = DWSClusterSensor(
        task_id="wait_restore_cluster_available",
        cluster_name=restore_cluster_name,
        target_status="AVAILABLE",
        poke_interval=15,
        timeout=60 * 30,
    )
    # [END howto_sensor_dws_wait_cluster_available]

    # [START howto_operator_dws_delete_snapshot]
    delete_snapshot = DWSDeleteClusterSnapshotOperator(
        task_id="delete_snapshot",
        snapshot_name=snapshot_name,
    )
    # [END howto_operator_dws_delete_snapshot]

    # [START howto_operator_dws_delete_snapshot]
    delete_snapshot_2 = DWSDeleteClusterSnapshotOperator(
        task_id="delete_snapshot_2",
        snapshot_name=snapshot_name_2,
    )
    # [END howto_operator_dws_delete_snapshot]

    # [START howto_operator_dws_delete_cluster_based_on_snapshot]
    delete_cluster_based_on_snapshot = DWSDeleteClusterBasedOnSnapshotOperator(
        task_id="delete_cluster_based_on_snapshot",
        snapshot_name=snapshot_name,
    )
    # [END howto_operator_dws_delete_cluster_based_on_snapshot]

    # [START howto_operator_dws_delete_cluster]
    delete_cluster_2 = DWSDeleteClusterOperator(
        task_id="delete_cluster_2",
        cluster_name=cluster_name_2,
        keep_last_manual_snapshot=0,
    )
    # [END howto_operator_dws_delete_cluster]

    create_cluster >> wait_cluster_available >> create_cluster_snapshot >> wait_snapshot_available \
        >> restore_cluster >> wait_restore_cluster_available >> delete_cluster_based_on_snapshot >> \
        delete_snapshot

    create_cluster_2 >> wait_cluster_available_2 >> create_cluster_snapshot_2 >> wait_snapshot_available_2 \
        >> delete_snapshot_2 >> delete_cluster_2

