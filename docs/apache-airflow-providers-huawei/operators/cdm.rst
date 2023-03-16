 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

==========================
Huawei Cloud CDM Operators
==========================

Overview
--------

Cloud Data Migration (CDM) is an efficient and easy-to-use batch data integration service.
Based on the big data migration to the cloud and intelligent data lake solution, CDM provides easy-to-use migration
capabilities and capabilities of integrating multiple data sources to the data lake, reducing the complexity
of data source migration and integration and effectively improving the data migration and integration efficiency.

 - :class:`~airflow.providers.huawei.cloud.operators.cdm.CDMCreateJobOperator`
 - :class:`~airflow.providers.huawei.cloud.operators.cdm.CDMCreateAndExecuteJobOperator`
 - :class:`~airflow.providers.huawei.cloud.operators.cdm.CDMStartJobOperator`
 - :class:`~airflow.providers.huawei.cloud.operators.cdm.CDMDeleteJobOperator`
 - :class:`~airflow.providers.huawei.cloud.operators.cdm.CDMStopJobOperator`
 - :class:`~airflow.providers.huawei.cloud.sensors.cdm.CDMShowJobStatusSensor`

Operators
---------

Create a job
==============

To create a job you can use
:class:`~airflow.providers.huawei.cloud.operators.cdm.CDMCreateJobOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_cdm.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_cdm_create_job]
   :end-before: [END howto_operator_cdm_create_job]

Create and execute job
======================

To create and execute a job you can use
:class:`~airflow.providers.huawei.cloud.operators.cdm.CDMCreateAndExecuteJobOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_cdm.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_cdm_create_and_execute_job]
   :end-before: [END howto_operator_cdm_create_and_execute_job]


Start a job
===========

To start a job you can use
:class:`~airflow.providers.huawei.cloud.operators.cdm.CDMStartJobOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_cdm.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_cdm_start_job]
   :end-before: [END howto_operator_cdm_start_job]

Stop a job
===========

To stop a job you can use
:class:`~airflow.providers.huawei.cloud.operators.cdm.CDMStopJobOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_cdm.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_cdm_stop_job]
   :end-before: [END howto_operator_cdm_stop_job]

Delete a job
============

To delete a job you can use
:class:`~airflow.providers.huawei.cloud.operators.cdm.CDMDeleteJobOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_cdm.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_cdm_delete_job]
   :end-before: [END howto_operator_cdm_delete_job]

Sensors
-------

Show job status
===================

Use the :class:`~airflow.providers.huawei.cloud.sensors.cdm.CDMShowJobStatusSensor`
Wait to query the status of a job.

.. exampleinclude:: /../../tests/system/providers/huawei/example_cdm.py
    :language: python
    :start-after: [START howto_sensor_cdm_show_job_status]
    :dedent: 4
    :end-before: [END howto_sensor_cdm_show_job_status]
