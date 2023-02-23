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

AK = "AK"
SK = "SK"
MOCK_REGION = "ap-southeast-3"
MOCK_BUCKET_NAME = "mock_bucket_name"
MOCK_PROJECT_ID = "mock_project_id"


def mock_huawei_cloud_default(self, huaweicloud_conn_id="mock_default_connection", region=MOCK_REGION):
    self.huaweicloud_conn_id = huaweicloud_conn_id
    self.conn = Connection(
        login=AK,
        password=SK,
        extra=json.dumps(
            {
                "region": region,
                "obs_bucket": MOCK_BUCKET_NAME,
                "project_id": MOCK_PROJECT_ID,
            }
        )
    )
