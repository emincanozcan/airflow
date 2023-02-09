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
Huawei Cloud DLI Operators
==========================

Overview
--------

Data Lake Insight (DLI) is a serverless data processing and analysis service fully compatible with Spark, Flink, and openLooKeng (Presto-based) ecosystems. 
Now, working with streaming and batch data and interactive analysis in data lakes and warehouses is effortless thanks to BI and AI capabilities.

 - :class:`~airflow.providers.huawei.cloud.operators.dli.DLICreateQueueOperator`
 - :class:`~airflow.providers.huawei.cloud.operators.dli.DLIDeleteQueueOperator`
 - :class:`~airflow.providers.huawei.cloud.operators.dli.DLIListQueuesOperator`
 - :class:`~airflow.providers.huawei.cloud.operators.dli.DLISparkCreateBatchJobOperator`
 - :class:`~airflow.providers.huawei.cloud.operators.dli.DLIUploadFilesOperator`
 - :class:`~airflow.providers.huawei.cloud.operators.dli.DLIRunSqlJobOperator`
 - :class:`~airflow.providers.huawei.cloud.operators.dli.DLIUpdateQueueCidrOperator`
 - :class:`~airflow.providers.huawei.cloud.operators.dli.DLIGetSqlJobResultOperator`

Operators
---------

Create a queue
==============

To create a queue you can use
:class:`~airflow.providers.huawei.cloud.operators.dli.DLICreateQueueOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dli.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_dli_create_queue]
   :end-before: [END howto_operator_dli_create_queue]


Delete a queue
==============

To delete a specified queue you can use
:class:`~airflow.providers.huawei.cloud.operators.dli.DLIDeleteQueueOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dli.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_dli_delete_queue]
   :end-before: [END howto_operator_dli_delete_queue]


List queues
===========

To list all queues under the project you can use
:class:`~airflow.providers.huawei.cloud.operators.dli.DLIListQueuesOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dli.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_dli_list_queue]
   :end-before: [END howto_operator_dli_list_queue]

Create a Spark batch job
========================

To create a Spark batch processing job in a queue you can use
:class:`~airflow.providers.huawei.cloud.operators.dli.DLISparkCreateBatchJobOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dli.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_dli_create_batch_job]
   :end-before: [END howto_operator_dli_create_batch_job]

Upload files
============

To upload a group of File packages to a project you can use
:class:`~airflow.providers.huawei.cloud.operators.dli.DLIUploadFilesOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dli.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_dli_upload_files]
   :end-before: [END howto_operator_dli_upload_files]

Run Sql job
===========

To submit jobs to a queue using SQL statements you can use.
:class:`~airflow.providers.huawei.cloud.operators.dli.DLIRunSqlJobOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dli.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_dli_run_job]
   :end-before: [END howto_operator_dli_run_job]

Get Sql job result
==================

To view the job execution result after a job is executed using SQL query statements you can use.
:class:`~airflow.providers.huawei.cloud.operators.dli.DLIGetSqlJobResultOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dli.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_dli_get_job_result]
   :end-before: [END howto_operator_dli_get_job_result]

Update queue CIDR
=================

To modify the CIDR block of the queues using the yearly/monthly packages you can use.
:class:`~airflow.providers.huawei.cloud.operators.dli.DLIUpdateQueueCidrOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dli.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_dli_update_queue_cidr]
   :end-before: [END howto_operator_dli_update_queue_cidr]

Sensors
-------

Show Spark batch state
======================

Use the :class:`~airflow.providers.huawei.cloud.sensors.dli.DLISparkShowBatchStateSensor`
Wait to obtain the execution status of a Spark batch processing job.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dli.py
    :language: python
    :start-after: [START howto_sensor_dli_show_batch_state]
    :dedent: 4
    :end-before: [END howto_sensor_dli_show_batch_state]

Show Sql job status
===================

Use the :class:`~airflow.providers.huawei.cloud.sensors.dli.DLISqlShowJobStatusSensor`
Wait to query the status of a submitted sql job.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dli.py
    :language: python
    :start-after: [START howto_sensor_dli_show_job_status]
    :dedent: 4
    :end-before: [END howto_sensor_dli_show_job_status]