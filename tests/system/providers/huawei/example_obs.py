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
from airflow.models.baseoperator import chain
from airflow.providers.huawei.cloud.operators.huawei_obs import (
    OBSCopyObjectOperator,
    OBSCreateBucketOperator,
    OBSCreateObjectOperator,
    OBSDeleteBatchObjectOperator,
    OBSDeleteBucketOperator,
    OBSDeleteBucketTaggingOperator,
    OBSDeleteObjectOperator,
    OBSGetBucketTaggingOperator,
    OBSGetObjectOperator,
    OBSListBucketOperator,
    OBSListObjectsOperator,
    OBSMoveObjectOperator,
    OBSSetBucketTaggingOperator,
)
from airflow.providers.huawei.cloud.sensors.huawei_obs_key import OBSObjectKeySensor

DAG_ID = "example_obs"


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2023, 1, 29),
    catchup=False,
    tags=["example"],
) as dag:
    bucket_name = "example-bucket"
    bucket_name_2 = "example-bucket-2"
    object_key_content = "example_object_content"
    object_key_file = "example_object_file"

    # [START howto_operator_obs_create_bucket]
    create_bucket = OBSCreateBucketOperator(task_id="create_bucket", bucket_name=bucket_name)
    # [END howto_operator_obs_create_bucket]

    # [START howto_operator_obs_create_bucket]
    create_bucket_2 = OBSCreateBucketOperator(task_id="create_bucket_2", bucket_name=bucket_name_2)
    # [END howto_operator_obs_create_bucket]

    # [START howto_operator_obs_list_bucket]
    list_bucket = OBSListBucketOperator(
        task_id="list_bucket",
    )
    # [END howto_operator_obs_list_bucket]

    # [START howto_operator_obs_set_bucket_tagging]
    set_bucket_tagging = OBSSetBucketTaggingOperator(
        task_id="set_bucket_tagging",
        bucket_name=bucket_name,
        tag_info={
            "type": "temp",
            "app": "nginx",
        },
    )
    # [END howto_operator_obs_set_bucket_tagging]

    # [START howto_operator_obs_get_bucket_tagging]
    get_bucket_tagging = OBSGetBucketTaggingOperator(
        task_id="get_bucket_tagging",
        bucket_name=bucket_name,
    )
    # [END howto_operator_obs_get_bucket_tagging]

    # [START howto_operator_obs_delete_bucket_tagging]
    delete_bucket_tagging = OBSDeleteBucketTaggingOperator(
        task_id="delete_bucket_tagging",
        bucket_name=bucket_name,
    )
    # [END howto_operator_obs_delete_bucket_tagging]

    # [START howto_operator_obs_create_object]
    create_object_content = OBSCreateObjectOperator(
        task_id="create_object_content",
        bucket_name=bucket_name,
        object_key=object_key_content,
        data="Beijing\nShanghai\nGuangzhou\nShenzhen",
    )
    # [END howto_operator_obs_create_object]

    # [START howto_operator_obs_create_object]
    create_object_file = OBSCreateObjectOperator(
        task_id="create_object_file",
        bucket_name=bucket_name,
        object_type="file",
        object_key=object_key_file,
        data="/tmp/upload/filename",
    )
    # [END howto_operator_obs_create_object]

    # [START howto_operator_obs_list_object]
    list_object = OBSListObjectsOperator(
        task_id="list_object",
        bucket_name=bucket_name,
    )
    # [END howto_operator_obs_list_object]

    # [START howto_sensor_obs_object_key_single]
    sensor_one_key = OBSObjectKeySensor(
        task_id="sensor_one_key", bucket_name=bucket_name, object_key=object_key_content
    )
    # [END howto_sensor_obs_object_key_single]

    # [START howto_sensor_obs_object_key_multiple]
    sensor_two_keys = OBSObjectKeySensor(
        task_id="sensor_two_keys", bucket_name=bucket_name, object_key=[object_key_content, object_key_file]
    )
    # [END howto_sensor_obs_object_key_multiple]

    # [START howto_operator_obs_get_object]
    get_object = OBSGetObjectOperator(
        task_id="get_object",
        bucket_name=bucket_name,
        object_key=object_key_content,
        download_path="/tmp/download/filename",
    )
    # [END howto_operator_obs_get_object]

    # [START howto_operator_obs_copy_object]
    copy_object = OBSCopyObjectOperator(
        task_id="copy_object",
        source_bucket_name=bucket_name,
        dest_bucket_name=bucket_name_2,
        source_object_key=object_key_content,
        dest_object_key=object_key_content,
    )
    # [END howto_operator_obs_copy_object]

    # [START howto_operator_obs_move_object]
    move_object = OBSMoveObjectOperator(
        task_id="move_object",
        source_bucket_name=bucket_name,
        dest_bucket_name=bucket_name_2,
        source_object_key=object_key_file,
        dest_object_key=object_key_file,
    )
    # [END howto_operator_obs_move_object]

    # [START howto_operator_obs_delete_object]
    delete_object = OBSDeleteObjectOperator(
        task_id="delete_object",
        bucket_name=bucket_name,
        object_key=object_key_content,
    )
    # [END howto_operator_obs_delete_object]

    # [START howto_operator_obs_delete_batch_object]
    delete_batch_object = OBSDeleteBatchObjectOperator(
        task_id="delete_batch_object",
        bucket_name=bucket_name,
        object_list=[object_key_content, object_key_file],
    )
    # [END howto_operator_obs_delete_batch_object]

    # [START howto_operator_obs_delete_batch_object]
    delete_batch_object_2 = OBSDeleteBatchObjectOperator(
        task_id="delete_batch_object",
        bucket_name=bucket_name_2,
        object_list=[object_key_content, object_key_file],
    )
    # [END howto_operator_obs_delete_batch_object]

    # [START howto_operator_obs_delete_bucket]
    delete_bucket = OBSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=bucket_name,
    )
    # [END howto_operator_obs_delete_bucket]

    # [START howto_operator_obs_delete_bucket]
    delete_bucket_2 = OBSDeleteBucketOperator(
        task_id="delete_bucket_2",
        bucket_name=bucket_name_2,
    )
    # [END howto_operator_obs_delete_bucket]

    chain(
        create_bucket,
        create_bucket_2,
        list_bucket,
        set_bucket_tagging,
        get_bucket_tagging,
        delete_bucket_tagging,
        create_object_content,
        create_object_file,
        list_object,
        [sensor_one_key, sensor_two_keys],
        get_object,
        copy_object,
        move_object,
        delete_object,
        delete_batch_object,
        delete_bucket,
        delete_batch_object_2,
        delete_bucket_2,
    )


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
