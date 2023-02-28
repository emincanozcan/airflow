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

import json

from airflow.models import Connection

default_mock_constants = {
    "AK": "AK",
    "SK": "SK",
    "PROJECT_ID": "project_id",
    "REGION": "ap-southeast-3",
    "CONN_ID": "mock_smn_default"
}

def mock_huawei_cloud_default(self, huaweicloud_conn_id=default_mock_constants["CONN_ID"], region=default_mock_constants["REGION"], project_id=default_mock_constants["PROJECT_ID"]):
    self.huaweicloud_conn_id = huaweicloud_conn_id
    self.project_id = project_id
    self.region = region
    self.conn = Connection(
        login=default_mock_constants["AK"],
        password=default_mock_constants["SK"],
        extra=json.dumps(
            {
                "region": region,
                "project_id": project_id,
            }
        )
    )


