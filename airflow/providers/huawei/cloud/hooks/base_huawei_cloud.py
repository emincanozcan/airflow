from __future__ import annotations
from typing import Any

from huaweicloudsdkcore.auth.credentials import GlobalCredentials
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkiam.v3 import IamClient, KeystoneListAuthDomainsRequest

import json

from airflow.hooks.base import BaseHook

class HuaweiBaseHook(BaseHook):

    conn_name_attr = "huaweicloud_conn_id"
    default_conn_name = "huaweicloud_default"
    conn_type = "huaweicloud"
    hook_name = "Huawei Cloud"

    def __init__(self,
                 region=None,
                 project_id=None,
                 huaweicloud_conn_id="huaweicloud_default",
                 *args,
                 **kwargs) -> None:
        self.huaweicloud_conn_id = huaweicloud_conn_id
        self.conn = self.get_connection(self.huaweicloud_conn_id)
        self.override_region = region
        self.override_project_id = project_id
        super().__init__(*args, **kwargs)
        
    def get_enterprise_project_id_from_extra_data(self) -> str:
        """
        Gets enterprise_project_id from the extra_config option in connection.
        """
        if self.conn.extra_dejson.get('enterprise_project_id', None) is not None:
            return self.conn.extra_dejson.get('enterprise_project_id', None)
        return None


    def get_project_id(self) -> str | None:
        """
        Gets project_id from the extra_config option in connection.
        """
        if self.override_project_id is not None:
            return self.override_project_id
        if self.conn.extra_dejson.get('project_id', None) is not None:
            return self.conn.extra_dejson.get('project_id', None)
        raise Exception(f"No project_id is specified for connection: {self.huaweicloud_conn_id}")

    def get_region(self) -> str:
        """Returns region for the hook."""
        if self.override_region is not None:
            return self.override_region
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
                        "obs_bucket": "your-obs-bucket-name"
                    },
                    indent=2,
                ),
            },
        }

    def test_connection(self):
        try:
            ak = self.conn.login
            sk = self.conn.password
            credentials = GlobalCredentials(ak, sk)

            client = IamClient.new_builder() \
                .with_credentials(credentials) \
                .with_endpoint(f"https://iam.myhuaweicloud.com") \
                .build()

            request = KeystoneListAuthDomainsRequest()
            client.keystone_list_auth_domains(request)
            return True, "Connection test succeeded!"
        except exceptions.ClientRequestException as e:
            return False, f"{e.error_code} {e.error_msg}"
