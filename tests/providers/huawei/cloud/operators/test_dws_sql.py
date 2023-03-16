from unittest import TestCase, mock

from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.huawei.cloud.operators.dws_sql import DWSSqlOperator


MOCK_CONTEXT = mock.Mock()
MOCK_TASK_ID = "test-dws-operator"
MOCK_DWS_CONN_ID = "mock_dws_conn_default"


class TestDWSSqlOperator(TestCase):
    @mock.patch("airflow.providers.huawei.cloud.operators.dws_sql.DWSSqlOperator.get_db_hook")
    def test_execute(self, mock_hook):
        hook = mock.Mock()
        mock_run = hook.run
        mock_hook.return_value = hook
        sql = mock.Mock()
        operator = DWSSqlOperator(
            task_id=MOCK_TASK_ID,
            dws_conn_id=MOCK_DWS_CONN_ID,
            sql=sql,
            autocommit=True,
        )

        operator.execute(MOCK_CONTEXT)

        mock_run.assert_called_once_with(
            sql=sql,
            autocommit=True,
            parameters=None,
            handler=fetch_all_handler,
            split_statements=False,
            return_last=True,
        )
