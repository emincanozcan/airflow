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

.. _howto/connection:dws:

Huawei Cloud DWS Connection
============================

Authenticating to Huawei Cloud
-------------------------------

Authentication may be performed using access key/secret key `for more information <https://support.huaweicloud.com/intl/en-us/devg-apisign/api-sign-securetoken.html>`_ .

Default Connection IDs
----------------------

The default connection ID is ``dws_default``.

Configuring the Connection
--------------------------

Host (optional)
  Specify the Huawei Cloud DWS cluster endpoint.

Schema (optional)
  Specify the Huawei Cloud DWS database name.

Login (optional)
  Specify the username to use for authentication with Huawei Cloud DWS.

Password (optional)
  Specify the password to use for authentication with Huawei Cloud DWS.

Port (optional)
  Specify the port to use to interact with Huawei Cloud DWS.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in
    Huawei Cloud DWS connection. For a complete list of supported parameters


Examples
--------------------------------

**Database Authentication**

* **Schema**: ``yourself-database``
* **Host**: ``example-cluster.dws.myhuaweiclouds.com``
* **Login**: ``dwsuser``
* **Password**: ``********``
* **Port**: ``8000``
