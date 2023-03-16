from __future__ import annotations

import warnings
from typing import Sequence
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


class DWSSqlOperator(SQLExecuteQueryOperator):
    """
    Executes SQL Statements against a Huawei Cloud DWS cluster

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param dws_conn_id: reference to
        :ref:`Huawei Cloud DWS connection id<howto/connection:dws>`
    :param parameters: (optional) the parameters to render the SQL query with.
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    """

    template_fields: Sequence[str] = (
        "sql",
        "conn_id",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "postgresql"}

    def __init__(self, *, dws_conn_id: str = "dws_default", **kwargs) -> None:
        super().__init__(conn_id=dws_conn_id, **kwargs)
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`.""",
            DeprecationWarning,
            stacklevel=2,
        )
