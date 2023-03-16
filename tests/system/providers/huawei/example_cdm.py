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
from airflow.providers.huawei.cloud.operators.cdm import (
    CDMCreateAndExecuteJobOperator,
    CDMCreateJobOperator,
    CDMDeleteJobOperator,
    CDMStartJobOperator,
    CDMStopJobOperator,
)
from airflow.providers.huawei.cloud.sensors.cdm import CDMShowJobStatusSensor

project_id = "ea31ff23328a4d6bbcca820076f7c606"
cluster_id = "cluster_id"
clusters = ["cluster_id"]
x_language = "x_lang"
job_name = "job_name"
jobs = []

with DAG(
    "cdm",
    description="Huawei Cloud CDM",
    start_date=datetime(2022, 10, 29),
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    # [START howto_operator_cdm_create_job]
    create_job = CDMCreateJobOperator(task_id="cdm_create_job", cluster_id=cluster_id, jobs=jobs)
    # [END howto_operator_cdm_create_job]

    # [START howto_operator_cdm_create_and_execute_job]
    create_and_execute_job = CDMCreateAndExecuteJobOperator(
        task_id="cdm_create_and_execute_job", x_language=x_language, clusters=clusters, jobs=jobs
    )
    # [END howto_operator_cdm_create_and_execute_job]

    # [START howto_operator_cdm_start_job]
    start_job = CDMStartJobOperator(task_id="cdm_start_job", cluster_id=cluster_id, job_name=job_name)
    # [END howto_operator_cdm_start_job]

    # [START howto_operator_cdm_stop_job]
    stop_job = CDMStopJobOperator(task_id="cdm_stop_job", cluster_id=cluster_id, job_name=job_name)
    # [END howto_operator_cdm_stop_job]

    # [START howto_operator_cdm_delete_job]
    delete_job = CDMDeleteJobOperator(task_id="cdm_delete_job", cluster_id=cluster_id, job_name=job_name)
    # [END howto_operator_cdm_delete_job]

    # [START howto_sensor_cdm_show_job_status]
    job_status_sensor = CDMShowJobStatusSensor(
        task_id="cdm_show_job_status", cluster_id=cluster_id, job_name=job_name
    )
    # [END howto_sensor_cdm_show_job_status]


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
