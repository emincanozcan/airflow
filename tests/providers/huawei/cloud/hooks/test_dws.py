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

from unittest import TestCase, mock
from airflow.exceptions import AirflowException
from airflow.providers.huawei.cloud.hooks.dws import DWSHook, DwsClient
from tests.providers.huawei.cloud.utils.hw_mock import mock_huawei_cloud_default, default_mock_constants


MOCK_PROJECT_ID = default_mock_constants["PROJECT_ID"]
AK = default_mock_constants["AK"]
SK = default_mock_constants["SK"]
DWS_STRING = "airflow.providers.huawei.cloud.hooks.dws.{}"
MOCK_DWS_CONN_ID = "mock_dws_conn_default"
MOCK_CLUSTER_NAME = "mock_cluster_name"
MOCK_SNAPSHOT_NAME = "mock_snapshot_name"
MOCK_NODE_TYPE = "mock_node_type"
MOCK_SUBNET_ID = "mock_subnet_id"
MOCK_SECURITY_GROUP_ID = "mock_security_group_id"
MOCK_VPC_ID = "mock_vpc_id"
MOCK_USER_NAME = "mock_user_name"
MOCK_USER_PWD = "mock_user_pwd"
MOCK_CLUSTER_ID = "mock_cluster_id"
MOCK_SNAPSHOT_ID = "mock_snapshot_id"
MOCK_PUBLIC_BIND_TYPE = "mock_public_bind_type"
MOCK_EIP_ID = "mock_eip_id"
MOCK_ENTERPRISE_PROJECT_ID = "mock_enterprise_project_id"
MOCK_DESC = "test"


class TestDWSHook(TestCase):
    def setUp(self):
        with mock.patch(
            DWS_STRING.format("DWSHook.__init__"),
            new=mock_huawei_cloud_default,
        ):
            self.hook = DWSHook(huaweicloud_conn_id=MOCK_DWS_CONN_ID)
            self.hook.project_id = MOCK_PROJECT_ID

    def test_get_credential(self):
        self.assertTupleEqual((AK, SK), self.hook.get_credential())

    @mock.patch(DWS_STRING.format("BasicCredentials"))
    @mock.patch(DWS_STRING.format("DwsClient"))
    def test_get_dws_client(self, mock_dws_client, mock_basic_credentials):
        mock_dws_client.return_value = DwsClient()

        self.hook.get_dws_client()

        mock_basic_credentials.assert_called_once_with(
            ak=AK,
            sk=SK,
            project_id=MOCK_PROJECT_ID
        )

    @mock.patch(DWS_STRING.format("DWSHook._get_cluster_id"))
    @mock.patch(DWS_STRING.format("ListClusterDetailsRequest"))
    @mock.patch(DWS_STRING.format("DWSHook.get_dws_client"))
    def test_get_cluster_status(self, mock_dws_client, mock_request, mock_cluster_id):
        mock_list_cluster_details = mock_dws_client.return_value.list_cluster_details
        resp_cluster_status = mock.Mock(cluster=mock.Mock(status="AVAILABLE"))
        mock_list_cluster_details.return_value = resp_cluster_status
        mock_cluster_id.return_value = MOCK_CLUSTER_ID

        self.hook.get_cluster_status(MOCK_CLUSTER_NAME)

        mock_list_cluster_details.assert_called_once_with(mock_request(MOCK_CLUSTER_ID))

    @mock.patch(DWS_STRING.format("DWSHook._get_snapshot_id"))
    @mock.patch(DWS_STRING.format("ListSnapshotDetailsRequest"))
    @mock.patch(DWS_STRING.format("DWSHook.get_dws_client"))
    def test_get_snapshot_status(self, mock_dws_client, mock_request, mock_snapshot_id):
        mock_list_snapshot_details = mock_dws_client.return_value.list_snapshot_details
        resp_snapshot_status = mock.Mock(snapshot=mock.Mock(status="AVAILABLE"))
        mock_list_snapshot_details.return_value = resp_snapshot_status
        mock_snapshot_id.return_value = MOCK_SNAPSHOT_ID

        self.hook.get_snapshot_status(MOCK_CLUSTER_ID)

        mock_list_snapshot_details.assert_called_once_with(mock_request(MOCK_CLUSTER_ID))

    @mock.patch(DWS_STRING.format("PublicIp"))
    @mock.patch(DWS_STRING.format("CreateClusterInfo"))
    @mock.patch(DWS_STRING.format("CreateClusterRequestBody"))
    @mock.patch(DWS_STRING.format("CreateClusterRequest"))
    @mock.patch(DWS_STRING.format("DWSHook.get_dws_client"))
    def test_create_cluster(self, mock_dws_client, mock_request, mock_body, mock_info, mock_public_ip):
        mock_create_cluster = mock_dws_client.return_value.create_cluster
        mock_create_cluster.return_value = mock.Mock(cluster=mock.Mock(id=MOCK_CLUSTER_ID))
        cluster_info = mock_info(
            name=MOCK_CLUSTER_NAME,
            node_type=MOCK_NODE_TYPE,
            number_of_node=3,
            subnet_id=MOCK_SUBNET_ID,
            security_group_id=MOCK_SECURITY_GROUP_ID,
            vpc_id=MOCK_VPC_ID,
            availability_zone=None,
            port=8000,
            user_name=MOCK_USER_NAME,
            user_pwd=MOCK_USER_PWD,
            public_ip=None,
            number_of_cn=None,
            enterprise_project_id=None,
        )
        request = mock_request(
            body=mock_body(cluster=cluster_info)
        )

        cluster_id = self.hook.create_cluster(
            name=MOCK_CLUSTER_NAME,
            node_type=MOCK_NODE_TYPE,
            number_of_node=3,
            subnet_id=MOCK_SUBNET_ID,
            security_group_id=MOCK_SECURITY_GROUP_ID,
            vpc_id=MOCK_VPC_ID,
            user_name=MOCK_USER_NAME,
            user_pwd=MOCK_USER_PWD,
            public_bind_type=None,
        )

        mock_public_ip.assert_not_called()
        mock_create_cluster.assert_called_once_with(request)
        self.assertEqual(MOCK_CLUSTER_ID, cluster_id)

    @mock.patch(DWS_STRING.format("DWSHook._get_cluster_id"))
    @mock.patch(DWS_STRING.format("DWSHook.set_cluster_snapshot_tag"))
    @mock.patch(DWS_STRING.format("DWSHook.get_cluster_status"))
    @mock.patch(DWS_STRING.format("Snapshot"))
    @mock.patch(DWS_STRING.format("CreateSnapshotRequestBody"))
    @mock.patch(DWS_STRING.format("CreateSnapshotRequest"))
    @mock.patch(DWS_STRING.format("DWSHook.get_dws_client"))
    def test_create_snapshot(self, mock_dws_client, mock_request, mock_body, mock_info, mock_status,
                             mock_tag, mock_cluster_id):
        mock_status.return_value = "AVAILABLE"
        mock_create_snapshot = mock_dws_client.return_value.create_snapshot
        mock_create_snapshot.return_value = mock.Mock(snapshot=mock.Mock(id=MOCK_SNAPSHOT_ID))
        mock_cluster_id.return_value = MOCK_CLUSTER_ID
        snapshot_info = mock_info(
            name=MOCK_SNAPSHOT_NAME,
            cluster_id=MOCK_CLUSTER_ID,
            description=MOCK_DESC
        )
        request = mock_request(
            body=mock_body(snapshot=snapshot_info)
        )
        snapshot_id = self.hook.create_snapshot(
            name=MOCK_SNAPSHOT_NAME,
            cluster_name=MOCK_CLUSTER_NAME,
            description=MOCK_DESC
        )
        mock_create_snapshot.assert_called_once_with(request)
        mock_tag.assert_called_once_with(
            cluster_id=MOCK_CLUSTER_ID,
            snapshot_id=MOCK_SNAPSHOT_ID
        )
        self.assertEqual(MOCK_SNAPSHOT_ID, snapshot_id)

    @mock.patch(DWS_STRING.format("DWSHook._get_cluster_id"))
    @mock.patch(DWS_STRING.format("DWSHook.get_cluster_status"))
    @mock.patch(DWS_STRING.format("DWSHook.get_dws_client"))
    def test_create_snapshot_if_status_not_available(self, mock_dws_client, mock_status, mock_cluster_id):
        mock_status.return_value = "UNAVAILABLE"
        mock_create_snapshot = mock_dws_client.return_value.create_snapshot
        mock_cluster_id.return_value = MOCK_CLUSTER_ID
        with self.assertRaises(AirflowException) as e:
            self.hook.create_snapshot(
                name=MOCK_SNAPSHOT_NAME,
                cluster_name=MOCK_CLUSTER_NAME,
                description=MOCK_DESC
            )
            mock_create_snapshot.assert_not_called()

    @mock.patch(DWS_STRING.format("DWSHook._get_snapshot_id"))
    @mock.patch(DWS_STRING.format("DeleteSnapshotRequest"))
    @mock.patch(DWS_STRING.format("DWSHook.get_dws_client"))
    def test_delete_snapshot(self, mock_dws_client, mock_request, mock_snapshot_id):
        mock_delete_snapshot = mock_dws_client.return_value.delete_snapshot
        mock_snapshot_id.return_value = MOCK_SNAPSHOT_ID
        request = mock_request(snapshot_id=MOCK_SNAPSHOT_ID)

        self.hook.delete_snapshot(snapshot_name=MOCK_SNAPSHOT_NAME)
        mock_delete_snapshot.assert_called_once_with(request)

    @mock.patch(DWS_STRING.format("DWSHook._get_cluster_id"))
    @mock.patch(DWS_STRING.format("DeleteClusterRequestBody"))
    @mock.patch(DWS_STRING.format("DeleteClusterRequest"))
    @mock.patch(DWS_STRING.format("DWSHook.get_dws_client"))
    def test_delete_cluster(self, mock_dws_client, mock_request, mock_body, mock_cluster_id):
        mock_delete_cluster = mock_dws_client.return_value.delete_cluster
        mock_cluster_id.return_value = MOCK_CLUSTER_ID
        request = mock_request(
            cluster_id=MOCK_CLUSTER_ID,
            body=mock_body(keep_last_manual_snapshot=3)
        )

        self.hook.delete_cluster(cluster_name=MOCK_CLUSTER_NAME, keep_last_manual_snapshot=3)
        mock_delete_cluster.assert_called_once_with(request)

    @mock.patch(DWS_STRING.format("DWSHook._get_snapshot_id"))
    @mock.patch(DWS_STRING.format("DWSHook.get_snapshot_tag_clusters"))
    @mock.patch(DWS_STRING.format("DeleteClusterRequestBody"))
    @mock.patch(DWS_STRING.format("DeleteClusterRequest"))
    @mock.patch(DWS_STRING.format("DWSHook.get_dws_client"))
    def test_delete_cluster_based_on_snapshot(self, mock_dws_client, mock_request, mock_body, mock_get_tag,
                                              mock_snapshot_id):
        mock_delete_cluster = mock_dws_client.return_value.delete_cluster
        mock_get_tag.return_value = [MOCK_CLUSTER_ID, "mock_cluster_id_2"]
        mock_snapshot_id.return_value = MOCK_SNAPSHOT_ID
        mock_request.side_effect = [
            mock.Mock(cluster_id=MOCK_CLUSTER_ID, body=mock_body()),
            mock.Mock(cluster_id="mock_cluster_id_2", body=mock_body()),
        ]

        self.hook.delete_cluster_based_on_snapshot(snapshot_name=MOCK_SNAPSHOT_NAME)

        mock_delete_cluster.assert_called()

    @mock.patch(DWS_STRING.format("ListClustersRequest"))
    @mock.patch(DWS_STRING.format("DWSHook.get_dws_client"))
    def test_get_snapshot_tag_clusters(self, mock_dws_client, mock_request):
        mock_list_clusters = mock_dws_client.return_value.list_clusters
        mock_list_clusters.return_value = mock.Mock(clusters=[
            mock.Mock(tags=[
                mock.Mock(key="snapshot_id", value=MOCK_SNAPSHOT_ID),
                mock.Mock(key="version", value="v1"),
            ], id=MOCK_CLUSTER_ID),
            mock.Mock(tags=[
                mock.Mock(key="snapshot_id", value=MOCK_SNAPSHOT_ID),
                mock.Mock(key="version", value="v2"),
            ], id="mock_cluster_id_2"),
            mock.Mock(tags=[
                mock.Mock(key="app", value="nginx"),
            ], id="mock_cluster_id_3"),
        ])
        cluster_ids = self.hook.get_snapshot_tag_clusters(snapshot_id=MOCK_SNAPSHOT_ID)

        mock_list_clusters.assert_called_once_with(mock_request())
        self.assertListEqual([MOCK_CLUSTER_ID, "mock_cluster_id_2"], cluster_ids)

    @mock.patch(DWS_STRING.format("DWSHook._get_snapshot_id"))
    @mock.patch(DWS_STRING.format("DWSHook.set_cluster_snapshot_tag"))
    @mock.patch(DWS_STRING.format("DWSHook.get_snapshot_status"))
    @mock.patch(DWS_STRING.format("PublicIp"))
    @mock.patch(DWS_STRING.format("Restore"))
    @mock.patch(DWS_STRING.format("RestoreClusterRequest"))
    @mock.patch(DWS_STRING.format("DWSHook.get_dws_client"))
    def test_restore_cluster(self, mock_dws_client, mock_request, mock_body, mock_public_ip,
                             mock_get_snapshot_status, mock_set_tag, mock_snapshot_id):
        mock_restore_cluster = mock_dws_client.return_value.restore_cluster
        mock_restore_cluster.return_value = mock.Mock(cluster=mock.Mock(id=MOCK_CLUSTER_ID))
        mock_get_snapshot_status.return_value = "AVAILABLE"
        mock_snapshot_id.return_value = MOCK_SNAPSHOT_ID
        restore = mock_body(
            name=MOCK_CLUSTER_NAME,
            subnet_id=MOCK_SUBNET_ID,
            security_group_id=MOCK_SECURITY_GROUP_ID,
            vpc_id=MOCK_VPC_ID,
            availability_zone=None,
            port=8000,
            public_ip=mock_public_ip(
                public_bind_type=MOCK_PUBLIC_BIND_TYPE,
                eip_id=MOCK_EIP_ID
            ),
            enterprise_project_id=MOCK_ENTERPRISE_PROJECT_ID,
        )
        request = mock_request(
            snapshot_id=MOCK_SNAPSHOT_ID,
            body=mock_body(restore)
        )

        cluster = self.hook.restore_cluster(
            snapshot_name=MOCK_SNAPSHOT_NAME,
            name=MOCK_CLUSTER_NAME,
            subnet_id=MOCK_SUBNET_ID,
            security_group_id=MOCK_SECURITY_GROUP_ID,
            vpc_id=MOCK_VPC_ID,
            availability_zone=None,
            port=8000,
            public_bind_type=MOCK_PUBLIC_BIND_TYPE,
            eip_id=MOCK_EIP_ID,
            enterprise_project_id=MOCK_ENTERPRISE_PROJECT_ID,

        )

        mock_get_snapshot_status.assert_called_once_with(MOCK_SNAPSHOT_NAME)
        mock_restore_cluster.assert_called_once_with(request)
        mock_set_tag.assert_called_once_with(
            cluster_id=MOCK_CLUSTER_ID,
            snapshot_id=MOCK_SNAPSHOT_ID
        )
        self.assertEqual(MOCK_CLUSTER_ID, cluster)

    @mock.patch(DWS_STRING.format("DWSHook._get_snapshot_id"))
    @mock.patch(DWS_STRING.format("DWSHook.set_cluster_snapshot_tag"))
    @mock.patch(DWS_STRING.format("DWSHook.get_snapshot_status"))
    @mock.patch(DWS_STRING.format("DWSHook.get_dws_client"))
    def test_restore_cluster_if_status_not_available(self, mock_dws_client, mock_get_snapshot_status,
                                                     mock_set_tag, mock_snapshot_id):
        mock_restore_cluster = mock_dws_client.return_value.restore_cluster
        mock_restore_cluster.return_value = mock.Mock(cluster=mock.Mock(id=MOCK_CLUSTER_ID))
        mock_get_snapshot_status.return_value = "UNAVAILABLE"
        mock_snapshot_id.return_value = MOCK_SNAPSHOT_ID

        cluster = self.hook.restore_cluster(
            snapshot_name=MOCK_SNAPSHOT_NAME,
            name=MOCK_CLUSTER_NAME,
        )

        mock_get_snapshot_status.assert_called_once_with(MOCK_SNAPSHOT_NAME)
        mock_restore_cluster.assert_not_called()
        mock_set_tag.assert_not_called()
        self.assertIsNone(cluster)

    @mock.patch(DWS_STRING.format("DWSHook.get_cluster_snapshot_tag"))
    @mock.patch(DWS_STRING.format("BatchCreateResourceTag"))
    @mock.patch(DWS_STRING.format("BatchCreateResourceTags"))
    @mock.patch(DWS_STRING.format("BatchCreateResourceTagRequest"))
    @mock.patch(DWS_STRING.format("DWSHook.get_dws_client"))
    def test_set_cluster_snapshot_tag(self, mock_dws_client, mock_request, mock_body, mock_tag,
                                      mock_get_tag):
        mock_set_cluster_snapshot_tag = mock_dws_client.return_value.batch_create_resource_tag
        mock_set_cluster_snapshot_tag.return_value = mock.Mock(cluster=mock.Mock(id=MOCK_CLUSTER_ID))
        mock_get_tag.return_value = ""
        request = mock_request(
            cluster_id=MOCK_CLUSTER_ID,
            body=mock_body([mock_tag(key="snapshot_id", value=MOCK_SNAPSHOT_ID)])
        )

        self.hook.set_cluster_snapshot_tag(
            cluster_id=MOCK_CLUSTER_ID,
            snapshot_id=MOCK_SNAPSHOT_ID,
        )

        mock_get_tag.assert_called_once_with(MOCK_CLUSTER_ID)
        mock_set_cluster_snapshot_tag.assert_called_once_with(request)

    @mock.patch(DWS_STRING.format("ListClusterTagsRequest"))
    @mock.patch(DWS_STRING.format("DWSHook.get_dws_client"))
    def test_get_cluster_snapshot_tag(self, mock_dws_client, mock_request):
        mock_list_cluster_tag = mock_dws_client.return_value.list_cluster_tags
        mock_list_cluster_tag.return_value = mock.Mock(tags=[
            mock.Mock(key="snapshot_id", value=MOCK_SNAPSHOT_ID),
            mock.Mock(key="key2", value="v2"),
        ])
        request = mock_request(MOCK_CLUSTER_ID)

        value = self.hook.get_cluster_snapshot_tag(MOCK_CLUSTER_ID)

        mock_list_cluster_tag.assert_called_once_with(request)
        self.assertEqual(MOCK_SNAPSHOT_ID, value)
