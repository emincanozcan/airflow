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

SMN_PROJECT_ID_HOOK_UNIT_TEST = "example-project"
PROJECT_ID = "project_id"
TOPIC_URN = "topic_urn"
AK = "AK"
SK = "SK"

def mock_smn_hook_default_project_id(self, huaweicloud_conn_id="mock_smn_default", region="ap-southeast-3"):
    self.huaweicloud_conn_id = huaweicloud_conn_id
    self.smn_conn = Connection(
        login=AK,
        password=SK,
        extra=json.dumps(
            {
                "region": region,
            }
        )
    )
    self.project_id = PROJECT_ID
    self.topic_urn = TOPIC_URN
    self.region = region

def response_text_message():
    return
