from unittest import TestCase, mock
from airflow.providers.huawei.cloud.operators.dws import (
    DWSCreateClusterOperator,
    DWSDeleteClusterOperator,
    DWSCreateClusterSnapshotOperator,
    DWSDeleteClusterSnapshotOperator,
    DWSRestoreClusterOperator,
    DWSDeleteClusterBasedOnSnapshotOperator
)

MOCK_TASK_ID = "test-dws-operator"
MOCK_DWS_CONN_ID = "mock_dws_conn_default"
MOCK_REGION = "mock_region"
MOCK_PROJECT_ID = "mock_project_id"
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

MOCK_CONTEXT = mock.Mock()


class TestDWSCreateClusterOperator(TestCase):

    @mock.patch("airflow.providers.huawei.cloud.operators.dws.DWSHook")
    def test_execute(self, mock_hook):
        operator = DWSCreateClusterOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_DWS_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            region=MOCK_REGION,
            name=MOCK_CLUSTER_NAME,
            node_type=MOCK_NODE_TYPE,
            number_of_node=3,
            subnet_id=MOCK_SUBNET_ID,
            security_group_id=MOCK_SECURITY_GROUP_ID,
            vpc_id=MOCK_VPC_ID,
            user_name=MOCK_USER_NAME,
            user_pwd=MOCK_USER_PWD,
        )

        operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DWS_CONN_ID,
            region=MOCK_REGION,
            project_id=MOCK_PROJECT_ID
        )
        mock_hook.return_value.create_cluster.assert_called_once_with(
            name=MOCK_CLUSTER_NAME,
            node_type=MOCK_NODE_TYPE,
            number_of_node=3,
            subnet_id=MOCK_SUBNET_ID,
            security_group_id=MOCK_SECURITY_GROUP_ID,
            vpc_id=MOCK_VPC_ID,
            user_name=MOCK_USER_NAME,
            user_pwd=MOCK_USER_PWD,
            port=8000,
            public_bind_type=None,
            eip_id=None,
            number_of_cn=None,
            enterprise_project_id=None,
            availability_zone=None
        )


class TestDWSCreateClusterSnapshotOperator(TestCase):

    @mock.patch("airflow.providers.huawei.cloud.operators.dws.DWSHook")
    def test_execute(self, mock_hook):
        operator = DWSCreateClusterSnapshotOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_DWS_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            region=MOCK_REGION,
            name=MOCK_SNAPSHOT_NAME,
            cluster_name=MOCK_CLUSTER_NAME,
            description="test"
        )

        operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DWS_CONN_ID,
            region=MOCK_REGION,
            project_id=MOCK_PROJECT_ID
        )
        mock_hook.return_value.create_snapshot.assert_called_once_with(
            name=MOCK_SNAPSHOT_NAME,
            cluster_name=MOCK_CLUSTER_NAME,
            description="test"
        )


class TestDWSDeleteClusterSnapshotOperator(TestCase):

    @mock.patch("airflow.providers.huawei.cloud.operators.dws.DWSHook")
    def test_execute(self, mock_hook):
        operator = DWSDeleteClusterSnapshotOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_DWS_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            region=MOCK_REGION,
            snapshot_name=MOCK_SNAPSHOT_NAME
        )

        operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DWS_CONN_ID,
            region=MOCK_REGION,
            project_id=MOCK_PROJECT_ID
        )
        mock_hook.return_value.delete_snapshot.assert_called_once_with(
            snapshot_name=MOCK_SNAPSHOT_NAME
        )


class TestDWSRestoreClusterOperator(TestCase):

    @mock.patch("airflow.providers.huawei.cloud.operators.dws.DWSHook")
    def test_execute(self, mock_hook):
        operator = DWSRestoreClusterOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_DWS_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            region=MOCK_REGION,
            snapshot_name=MOCK_SNAPSHOT_NAME,
            name=MOCK_CLUSTER_NAME,
            subnet_id=MOCK_SUBNET_ID,
            security_group_id=MOCK_SECURITY_GROUP_ID,
            vpc_id=MOCK_VPC_ID,
        )

        operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DWS_CONN_ID,
            region=MOCK_REGION,
            project_id=MOCK_PROJECT_ID
        )
        mock_hook.return_value.restore_cluster.assert_called_once_with(
            snapshot_name=MOCK_SNAPSHOT_NAME,
            name=MOCK_CLUSTER_NAME,
            subnet_id=MOCK_SUBNET_ID,
            security_group_id=MOCK_SECURITY_GROUP_ID,
            vpc_id=MOCK_VPC_ID,
            availability_zone=None,
            port=8000,
            public_bind_type=None,
            eip_id=None,
            enterprise_project_id=None,
        )


class TestDWSDeleteClusterBasedOnSnapshotOperator(TestCase):

    @mock.patch("airflow.providers.huawei.cloud.operators.dws.DWSHook")
    def test_execute(self, mock_hook):
        operator = DWSDeleteClusterBasedOnSnapshotOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_DWS_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            region=MOCK_REGION,
            snapshot_name=MOCK_SNAPSHOT_NAME
        )

        operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DWS_CONN_ID,
            region=MOCK_REGION,
            project_id=MOCK_PROJECT_ID
        )
        mock_hook.return_value.delete_cluster_based_on_snapshot.assert_called_once_with(
            snapshot_name=MOCK_SNAPSHOT_NAME
        )


class TestDWSDeleteClusterOperator(TestCase):

    @mock.patch("airflow.providers.huawei.cloud.operators.dws.DWSHook")
    def test_execute(self, mock_hook):
        operator = DWSDeleteClusterOperator(
            task_id=MOCK_TASK_ID,
            huaweicloud_conn_id=MOCK_DWS_CONN_ID,
            project_id=MOCK_PROJECT_ID,
            region=MOCK_REGION,
            cluster_name=MOCK_CLUSTER_NAME,
            keep_last_manual_snapshot=3
        )

        operator.execute(MOCK_CONTEXT)

        mock_hook.assert_called_once_with(
            huaweicloud_conn_id=MOCK_DWS_CONN_ID,
            region=MOCK_REGION,
            project_id=MOCK_PROJECT_ID
        )
        mock_hook.return_value.delete_cluster.assert_called_once_with(
            cluster_name=MOCK_CLUSTER_NAME,
            keep_last_manual_snapshot=3,
        )
