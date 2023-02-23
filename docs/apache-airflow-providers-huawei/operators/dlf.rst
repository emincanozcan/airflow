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
Huawei Cloud DLF Operators
==========================

Overview
--------

Data Lake Factory (DataArts Factory) is a big data platform designed specifically for the HUAWEI CLOUD. It manages diverse big data services and provides a one-stop big data development environment and fully-managed big data scheduling capabilities. Thanks to DLF, big data is more accessible than ever before, helping you effortlessly build big data processing centers.

DataArts Factory enables a variety of operations such as data management, data integration, script development, job scheduling, and monitoring, facilitating the data analysis and processing procedure.

 - :class:`~airflow.providers.huawei.cloud.operators.dataarts.DLFStartJobOperator`

Operators
---------

Start a job
===========

To start a job you can use
:class:`~airflow.providers.huawei.cloud.operators.dataarts.DLFStartJobOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dlf.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_dlf_start_job]
   :end-before: [END howto_operator_dlf_start_job]

Sensors
-------

Show job status
===================

Use the :class:`~airflow.providers.huawei.cloud.sensors.dlf.DLFShowJobStatusSensor`
Wait to query the status of a job.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dlf.py
    :language: python
    :start-after: [START howto_sensor_dlf_show_job_status]
    :dedent: 4
    :end-before: [END howto_sensor_dlf_show_job_status]