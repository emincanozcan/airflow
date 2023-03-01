 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on a
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

==============================
Huawei Cloud DWS SQL Operators
==============================

Airflow to `Data Warehouse Service (DWS) <https://support.huaweicloud.com/intl/en-us/dws/>`__ integration provides operator to execute SQL statements.

Operators
---------

Executes SQL statements against a Huawei Cloud DWS cluster
==========================================================

To execute SQL statements against a Huawei Cloud DWS cluster.
:class:`~airflow.providers.huawei.cloud.operators.dws.DWSSqlOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dws_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dws_sql_create_table]
    :end-before: [END howto_operator_dws_sql_create_table]
