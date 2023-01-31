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
Huawei Cloud OBS Operators
==========================

Airflow to Huawei Cloud Object Storage Service (OBS) integration provides several operators to manage the life cycle of OBS buckets and objects.

Operators
---------

.. _howto/operator: OBSCreateBucketOperator:

Create a Huawei Cloud OBS bucket
================================

To create a Huawei Cloud OBS bucket you can use
:class:`~airflow.providers.huawei.cloud.operators.huawei_obs.OBSCreateBucketOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_obs_create_bucket]
   :end-before: [END howto_operator_obs_create_bucket]

.. _howto/operator:OBSListBucketOperator:

List Huawei Cloud OBS bucket
============================

To list Huawei Cloud OBS bucket you can use
:class:`~airflow.providers.huawei.cloud.operators.huawei_obs.OBSListBucketOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_obs_list_bucket]
   :end-before: [END howto_operator_obs_list_bucket]

.. _howto/operator:OBSDeleteBucketOperator:

Delete a Huawei Cloud OBS bucket
================================

To delete a Huawei Cloud OBS bucket you can use
:class:`~airflow.providers.huawei.operators.huawei_obs.OBSDeleteBucketOperator`.

Non-empty buckets cannot be deleted directly.

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_obs_delete_bucket]
    :end-before: [END howto_operator_obs_delete_bucket]

.. _howto/operator:OBSSetBucketTaggingOperator:

Set the tags for a Huawei Cloud OBS bucket
==========================================

To set the tags for a Huawei Cloud OBS bucket you can use
:class:`~airflow.providers.huawei.operators.huawei_obs.OBSSetBucketTaggingOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_obs_set_bucket_tagging]
    :end-before: [END howto_operator_obs_set_bucket_tagging]

.. _howto/operator:OBSGetBucketTaggingOperator:

Get the tag of a Huawei Cloud OBS bucket
========================================

To get the tag set associated with a Huawei Cloud OBS bucket you can use
:class:`~airflow.providers.huawei.operators.huawei_obs.OBSGetBucketTaggingOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_obs_get_bucket_tagging]
    :end-before: [END howto_operator_obs_get_bucket_tagging]

.. _howto/operator:OBSDeleteBucketTaggingOperator:

Delete the tags of a Huawei Cloud OBS bucket
============================================

To delete the tags of a Huawei Cloud OBS bucket you can use
:class:`~airflow.providers.huawei.operators.huawei_obs.OBSDeleteBucketTaggingOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_obs_delete_bucket_tagging]
    :end-before: [END howto_operator_obs_delete_bucket_tagging]

.. _howto/operator:OBSCreateObjectOperator:

Create a Huawei Cloud OBS object
================================

To create a new (or replace) Huawei Cloud OBS object you can use
:class:`~airflow.providers.huawei.operators.huawei_obs.OBSCreateObjectOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_obs_create_object]
    :end-before: [END howto_operator_obs_create_object]

.. _howto/operator:OBSGetObjectOperator:

get a Huawei Cloud OBS object
=============================

To get a Huawei Cloud OBS object you can use
:class:`~airflow.providers.huawei.operators.huawei_obs.OBSGetObjectOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_obs_get_object]
    :end-before: [END howto_operator_obs_get_object]

.. _howto/operator:OBSCopyObjectOperator:

Copy a Huawei Cloud OBS object
==============================

To copy a Huawei Cloud OBS object from one bucket to another you can use
:class:`~airflow.providers.huawei.operators.huawei_obs.OBSCopyObjectOperator`.
The Huawei Cloud OBS connection used here needs to have access to both source and destination bucket/key.
Inter-region copy is not supported.

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_obs_copy_object]
    :end-before: [END howto_operator_obs_copy_object]

.. _howto/operator:OBSMoveObjectOperator:

To move a Huawei Cloud OBS object from one bucket to another you can use
:class:`~airflow.providers.huawei.operators.huawei_obs.OBSMoveObjectOperator`.
The Huawei Cloud OBS connection used here needs to have access to both source and destination bucket/key.
Inter-region move is not supported.

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_obs_move_object]
    :end-before: [END howto_operator_obs_move_object]

.. _howto/operator:OBSDeleteObjectOperator:

Delete a Huawei Cloud OBS objects
=================================

To delete a Huawei Cloud OBS object you can use
:class:`~airflow.providers.huawei.operators.huawei_obs.OBSDeleteObjectsOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_obs_delete_object]
    :end-before: [END howto_operator_obs_delete_object]

.. _howto/operator:OBSDeleteBatchObjectOperator:

Delete Huawei Cloud OBS objects
===============================

To delete one or multiple Huawei Cloud OBS objects you can use
:class:`~airflow.providers.huawei.operators.huawei_obs.OBSDeleteBatchObjectOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_obs_delete_batch_object]
    :end-before: [END howto_operator_obs_delete_batch_object]

.. _howto/operator:OBSListObjectsOperator:

List Huawei Cloud OBS objects
=============================

To list Huawei Cloud OBS objects within a Huawei Cloud OBS bucket you can use
:class:`~airflow.providers.huawei.operators.huawei_obs.OBSListObjectsOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_obs_list_object]
    :end-before: [END howto_operator_obs_list_object]

Sensors
-------

.. _howto/sensor:OBSObjectKeySensor:

Wait on Huawei Cloud OBS object keys
====================================

To wait for one or multiple object keys to be present in a Huawei Cloud OBS bucket you can use
:class:`~airflow.providers.huawei.sensors.huawei_obs.OBSObjectKeySensor`.

To check one file:

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_obs_object_key_single]
    :end-before: [END howto_sensor_obs_object_key_single]

To check multiple files:

.. exampleinclude:: /../../tests/system/providers/huawei/example_obs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_obs_object_key_multiple]
    :end-before: [END howto_sensor_obs_object_key_multiple]
