from datetime import datetime
from airflow import DAG


from airflow.providers.huawei.cloud.operators.smn import (
    SMNPublishTextMessageOperator,
    SMNPublishJsonMessageOperator,
    SMNPublishMessageTemplateOperator,
)


with DAG(
    "smn",
    description="Huawei Cloud SMN",
    schedule_interval="@once",
    start_date=datetime(2022, 10, 29),
    catchup=False,
) as dag:
    # [START howto_operator_smn_json_message]
    smn_json_operator = SMNPublishJsonMessageOperator(
        task_id="smn_task_json",
        huaweicloud_conn_id="huaweicloud_default",
        topic_urn="your_topic_urn",
        subject="your_subject",
        region="your_region",
        default="Default Message",
        sms="Sms Message",  # Optional, default will be used if None
        email="Email Message",  # Optional, default will be used if None
        http="Http Message",  # Optional, default will be used if None
        https="Https Message",  # Optional, default will be used if None
        functionstage="Function Stage Message",  # Optional, default will be used if None
    )
    # [END howto_operator_smn_json_message]
    # [START howto_operator_smn_text_message]
    smn_text_operator = SMNPublishTextMessageOperator(
        task_id="smn_task_text",
        huaweicloud_conn_id="huaweicloud_default",
        topic_urn="your_topic_urn",
        region="your_region",
        message="your_message",
        subject="your_subject",
    )
    # [END howto_operator_smn_text_message]
    # [START howto_operator_smn_message_template]
    smn_template_operator = SMNPublishMessageTemplateOperator(
        task_id="smn_task_template",
        huaweicloud_conn_id="huaweicloud_default",
        topic_urn="your_topic_urn",
        region="your_region",
        tags={"your_param": "your_input"},
        template_name="your_template_name",
        subject="your_subject",
    )
    # [END howto_operator_smn_message_template]
    smn_template_operator >> smn_json_operator >> smn_text_operator
