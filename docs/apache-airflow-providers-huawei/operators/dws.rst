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

==========================
Huawei Cloud DWS Operators
==========================

Airflow to `Data Warehouse Service (DWS) <https://support.huaweicloud.com/intl/en-us/dws/>`__ integration provides
several operators to manages all the work of setting up, operating, and scaling a data warehouse.

Operators
---------

.. _howto/operator: DWSCreateClusterOperator:

Create a Huawei Cloud DWS cluster
=================================

To create a Huawei Cloud DWS Cluster with the specified parameters you can use.
:class:`~airflow.providers.huawei.cloud.operators.dws.DWSCreateClusterOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dws.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dws_create_cluster]
    :end-before: [END howto_operator_dws_create_cluster]

.. _howto/operator: DWSCreateClusterSnapshotOperator:

Create a Huawei Cloud DWS cluster snapshot
===========================================

To create a Huawei Cloud DWS Cluster snapshot with the specified parameters you can use.
:class:`~airflow.providers.huawei.cloud.operators.dws.DWSCreateClusterSnapshotOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dws.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dws_create_cluster_snapshot]
    :end-before: [END howto_operator_dws_create_cluster_snapshot]

.. _howto/operator: DWSDeleteClusterSnapshotOperator:

Delete a Huawei Cloud DWS cluster snapshot
===========================================

To delete a Huawei Cloud DWS cluster snapshot with the specified parameters you can use.
:class:`~airflow.providers.huawei.cloud.operators.dws.DWSDeleteClusterSnapshotOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dws.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dws_delete_snapshot]
    :end-before: [END howto_operator_dws_delete_snapshot]

.. _howto/operator: DWSRestoreClusterOperator:

Restore a Huawei Cloud DWS cluster using a snapshot
====================================================

To Restore a Huawei Cloud DWS cluster using a snapshot.
:class:`~airflow.providers.huawei.cloud.operators.dws.DWSRestoreClusterOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dws.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dws_restore_cluster]
    :end-before: [END howto_operator_dws_restore_cluster]

.. _howto/operator: DWSDeleteClusterBasedOnSnapshotOperator:

Delete Huawei Cloud DWS clusters based on snapshot.Filter by cluster tags
=========================================================================

To Delete clusters based on snapshot.Filter by cluster tags.
:class:`~airflow.providers.huawei.cloud.operators.dws.DWSDeleteClusterBasedOnSnapshotOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dws.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dws_delete_cluster_based_on_snapshot]
    :end-before: [END howto_operator_dws_delete_cluster_based_on_snapshot]

.. _howto/operator: DWSDeleteClusterOperator:

Delete a Huawei Cloud DWS cluster
=================================

To Delete a Huawei Cloud DWS cluster.
:class:`~airflow.providers.huawei.cloud.operators.dws.DWSDeleteClusterOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dws.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dws_delete_cluster]
    :end-before: [END howto_operator_dws_delete_cluster]

Sensors
-------

.. _howto/sensor: DWSClusterSensor:

Waits for a DWS cluster to reach a specific status
==================================================

To wait for a DWS cluster to reach a specific status.
:class:`~airflow.providers.huawei.cloud.sensors.dws.DWSClusterSensor`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dws.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_dws_wait_cluster_available]
    :end-before: [END howto_sensor_dws_wait_cluster_available]

.. _howto/sensor: DWSSnapshotSensor:

Waits for a DWS snapshot to reach a specific status
==================================================

To wait for a DWS cluster to reach a specific status.
:class:`~airflow.providers.huawei.cloud.sensors.dws.DWSSnapshotSensor`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_dws.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_dws_wait_snapshot_available]
    :end-before: [END howto_sensor_dws_wait_snapshot_available]
