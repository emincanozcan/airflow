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
Huawei Cloud SMN Operators
==========================

Overview
--------

Airflow to Huawei Cloud Simple Message Notification (SMN) integration provides several operators to publish and interact with SMN.

 - :class:`~airflow.providers.huawei.cloud.operators.smn.SMNPublishMessageTemplateOperator`
 - :class:`~airflow.providers.huawei.cloud.operators.smn.SMNPublishTextMessageOperator`
 - :class:`~airflow.providers.huawei.cloud.operators.smn.SMNPublishJsonMessageOperator`

Purpose
-------

This example dag uses ``SMNPublishMessageTemplateOperator``, ``SMNPublishJsonMessageOperator`` and ``SMNPublishTextMessageOperator`` to publish a
new message to use Huawei Cloud SMN Operator. This Operator lets you send messages at scale to various endpoints, such as HTTP/HTTPS servers, email addresses, phone numbers, functions, and instant messaging tools.

Operators
---------

Publish a SMN text message
==========================

To publish a SMN text message you can use
:class:`~airflow.providers.huawei.cloud.operators.smn.SMNPublishTextMessageOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_smn.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_smn_text_message]
   :end-before: [END howto_operator_smn_text_message]


Publish a SMN json message
==========================

To publish a SMN json message you can use
:class:`~airflow.providers.huawei.cloud.operators.smn.SMNPublishJsonMessageOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_smn.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_smn_json_message]
   :end-before: [END howto_operator_smn_json_message]


Publish a SMN message template
==============================

To publish a SMN message template you can use
:class:`~airflow.providers.huawei.cloud.operators.smn.SMNPublishMessageTemplateOperator`.

.. exampleinclude:: /../../tests/system/providers/huawei/example_smn.py
   :dedent: 4
   :language: python
   :start-after: [START howto_operator_smn_message_template]
   :end-before: [END howto_operator_smn_message_template]

