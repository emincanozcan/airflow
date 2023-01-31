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

Huawei Cloud Connection
========================

Authenticating to Huawei Cloud
-------------------------------

Authentication may be performed using access key/secret key `for more information <https://support.huaweicloud.com/intl/en-us/devg-apisign/api-sign-securetoken.html>`_ .

Default Connection IDs
----------------------

The default connection ID is ``huaweicloud_default``.

Configuring the Connection
--------------------------

Huawei Cloud Access Key ID
    Specify the Huawei Cloud access key ID used for the initial connection.

Huawei Cloud Secret Access Key
    Specify the Huawei Cloud secret access key used for the initial connection.

Bucket Name
    The default bucket name used by the OBS operator.
    
Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Huawei Cloud
    connection. The following parameters are all optional:

    * ``region``: Regions where the API is available. It can be entered Airflow connection UI.
    

Examples for the **Extra** field
--------------------------------

.. code-block:: json

    {
      "region": "cn-south-1"
    }
