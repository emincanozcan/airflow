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
from airflow.providers.huawei.cloud.operators.dataarts import DataArtsDLFStartJobOperator
from airflow.providers.huawei.cloud.sensors.dataarts import DataArtsDLFShowJobStatusSensor

project_id = "ea31ff23328a4d6bbcca820076f7c606"
job_name = "job_name"
workspace = "workspace-id"
body = {"jobParams": [{"name": "param1", "value": "value1"}]}


with DAG(
    "dlf",
    description="Huawei Cloud DLF",
    start_date=datetime(2022, 10, 29),
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    # [START howto_operator_dlf_start_job]
    start_job = DataArtsDLFStartJobOperator(
        task_id="dlf_start_job", workspace=workspace, body=body, job_name=job_name
    )
    # [END howto_operator_dlf_start_job]

    # [START howto_sensor_dlf_show_job_status]
    job_status_sensor = DataArtsDLFShowJobStatusSensor(
        task_id="dlf_show_job_status", job_name=job_name, workspace=workspace
    )
    # [END howto_sensor_dlf_show_job_status]


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
