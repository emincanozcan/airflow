from __future__ import annotations
from typing import Any

import json

from airflow.hooks.base import BaseHook

class HuaweiBaseHook(BaseHook):
    
    conn_name_attr = "huaweicloud_conn_id"
    default_conn_name = "huaweicloud_default"
    conn_type = "huaweicloud"
    hook_name = "Huawei Cloud"
    
    def __init__(self,
                 region,
                 project_id,
                 huaweicloud_conn_id="huaweicloud_default",
                 *args,
                 **kwargs) -> None:
        self.huaweicloud_conn_id = huaweicloud_conn_id
        self.region = self.get_default_region() if region is None else region
        self.project_id = self.get_default_project_id() if project_id is None else project_id
        self.conn = self.get_connection(self.huaweicloud_conn_id)
        super().__init__(*args, **kwargs)
        
    
    def get_default_project_id(self) -> str | None:
        """
        Gets project_id from the extra_config option in connection.
        """
        
        if self.conn.extra_dejson.get('project_id', None) is not None:
            return self.conn.extra_dejson.get('project_id', None)
        raise Exception(f"No project_id is specified for connection: {self.huaweicloud_conn_id}")
    
    def get_default_region(self) -> str:
        """Returns region for the hook."""
        if self.conn.extra_dejson.get('region', None) is not None:
            return self.conn.extra_dejson.get('region', None)
        raise Exception(f"No region is specified for connection")
    
    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom UI field behaviour for Huawei Cloud Connection."""
        return {
            "hidden_fields": ["host", "schema", "port"],
            "relabeling": {
                "login": "Huawei Cloud Access Key ID",
                "password": "Huawei Cloud Secret Access Key",
            },
            "placeholders": {
                "login": "AKIAIOSFODNN7EXAMPLE",
                "password": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "extra": json.dumps(
                    {
                        "region": "ap-southeast-3",
                        "project_id": "1234567890",
                    },
                    indent=2,
                ),
            },
        }
    
    def test_connection(self):
        try:
            return True, self.get_default_region()
        except Exception as e:
            return False, str(f"{type(e).__name__!r} error occurred while testing connection: {e}")