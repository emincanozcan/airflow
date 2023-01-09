#
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
"""This module contains Alibaba Cloud OBS operators."""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.huawei.cloud.hooks.huawei_obs import OBSHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OBSCreateBucketOperator(BaseOperator):
    """
    This operator creates an OBS bucket on the region which in the obs_conn_id

    :param obs_conn_id: The Airflow connection used for OBS credentials.
        If this is None or empty then the default obs behaviour is used. If
            running Airflow in a distributed manner and aws_conn_id is None or
            empty, then default obs configuration would be used (and must be
            maintained on each worker node).
    :param region: OBS region you want to create bucket.
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可创建任意区域的桶
    :param bucket_name: This is bucket name you want to create.
    """

    template_fields: Sequence[str] = ("bucket_name",)

    def __init__(
            self,
            region: str | None = None,
            bucket_name: str | None = None,
            obs_conn_id: str | None = "obs_default",
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.obs_conn_id = obs_conn_id
        self.bucket_name = bucket_name
        self.region = region

    def execute(self, context: Context):
        obs_hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)
        obs_hook.create_bucket(bucket_name=self.bucket_name)


class OBSListBucketOperator(BaseOperator):
    """
    This operator list all OBS buckets on region.
    This operator returns a python list with the name of buckets which can be
    used by `xcom` in the downstream task.


    :param obs_conn_id: The Airflow connection used for OBS credentials.
    :param region: OBS region you want to list bucket
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可列出所有区域的桶名
    """

    def __init__(
            self,
            region: str | None = None,
            obs_conn_id: str | None = "obs_default",
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.obs_conn_id = obs_conn_id
        self.region = region

    def execute(self, context: Context):
        obs_hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)
        return obs_hook.list_bucket()


class OBSDeleteBucketOperator(BaseOperator):
    """
    This operator to delete an OBS bucket

    :param obs_conn_id: The Airflow connection used for OBS credentials.
    :param region: OBS region you want to delete bucket
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可删除任意区域的桶名
    :param bucket_name: This is bucket name you want to delete
    """

    template_fields: Sequence[str] = ("bucket_name",)

    def __init__(
            self,
            region: str | None = None,
            bucket_name: str | None = None,
            obs_conn_id: str | None = "obs_default",
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.obs_conn_id = obs_conn_id
        self.region = region
        self.bucket_name = bucket_name

    def execute(self, context: Context):
        obs_hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)
        if obs_hook.exist_bucket(self.bucket_name):
            obs_hook.delete_bucket(bucket_name=self.bucket_name)
        else:
            self.log.warning("OBS Bucket with name: %s doesn't exist on region: %s", self.bucket_name, obs_hook.region)


class OBSListBucketObjectsOperator(BaseOperator):
    """
    List all objects from the bucket with the given string prefix in name.
    This operator returns a python list with the name of objects which can be
    used by `xcom` in the downstream task.

    :param obs_conn_id: The Airflow connection used for OBS credentials.
    :param region: OBS region you want to list the objects for the bucket
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可列举任意区域的桶对象名
    :param bucket_name: This is bucket name you want to list all objects
    :param prefix: 限定返回的对象名必须带有prefix前缀。
    :param marker: 列举桶内对象列表时，指定一个标识符，从该标识符以后按字典顺序返回对象列表。
    :param max_keys: 列举对象的最大数目，取值范围为1~1000，当超出范围时，按照默认的1000进行处理。
    :param is_truncated: 是否开启分页查询，列举符合条件的所有对象
    """

    template_fields: Sequence[str] = ("bucket_name", "prefix", "marker", "max_keys",)
    ui_color = "#ffd700"

    def __init__(
            self,
            bucket_name: str | None = None,
            region: str | None = None,
            prefix: str | None = None,
            marker: str | None = None,
            max_keys: int | None = None,
            is_truncated: bool | None = False,
            obs_conn_id: str | None = "obs_default",
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.obs_conn_id = obs_conn_id
        self.region = region
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.marker = marker
        self.max_keys = max_keys
        self.is_truncated = is_truncated

    def execute(self, context: Context):
        obs_hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)

        if obs_hook.exist_bucket(self.bucket_name):

            return obs_hook.list_object(
                bucket_name=self.bucket_name,
                prefix=self.prefix,
                marker=self.marker,
                max_keys=self.max_keys,
                is_truncated=self.is_truncated,
            )
        else:
            self.log.warning("OBS Bucket with name: %s doesn't exist on region: %s", self.bucket_name, obs_hook.region)
            return None


class OBSGetBucketTaggingOperator(BaseOperator):
    """
    This operator get OBS bucket tagging

    :param obs_conn_id: The Airflow connection used for OBS credentials.
    :param region: OBS region you want to get bucket tagging
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可获取任意区域的桶标签
    :param bucket_name: This is bucket name you want to get bucket tagging.
    """

    template_fields: Sequence[str] = ("bucket_name",)

    def __init__(
            self,
            obs_conn_id: str | None = "obs_default",
            region: str | None = None,
            bucket_name: str | None = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.obs_conn_id = obs_conn_id
        self.region = region
        self.bucket_name = bucket_name

    def execute(self, context: Context):
        obs_hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)

        if obs_hook.exist_bucket(self.bucket_name):
            return obs_hook.get_bucket_tagging(self.bucket_name)
        else:
            self.log.warning("OBS Bucket with name: %s doesn't exist on region: %s", self.bucket_name, obs_hook.region)
            return None


class OBSSetBucketTaggingOperator(BaseOperator):
    """
    This operator set OBS bucket tagging
    进行该操作，会重置桶标签

    :param obs_conn_id: The Airflow connection used for OBS credentials.
    :param region: OBS region you want to set bucket tagging
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可设置任意区域的桶标签
    :param bucket_name: This is bucket name you want to set bucket tagging.
    :param tag_info: 桶标签配置
    """

    template_fields: Sequence[str] = ("bucket_name",)
    template_fields_renderers = {"tag_info": "json"}

    def __init__(
            self,
            tag_info: list[dict[str, str]] | None = None,
            bucket_name: str | None = None,
            obs_conn_id: str | None = "obs_default",
            region: str | None = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.obs_conn_id = obs_conn_id
        self.bucket_name = bucket_name
        self.tag_info = tag_info
        self.region = region

    def execute(self, context: Context):
        obs_hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)
        if obs_hook.exist_bucket(self.bucket_name):
            obs_hook.set_bucket_tagging(bucket_name=self.bucket_name, tag_info=self.tag_info)
        else:
            self.log.warning("OBS Bucket with name: %s doesn't exist on region: %s", self.bucket_name, obs_hook.region)


class OBSDeleteBucketTaggingOperator(BaseOperator):
    """
    This operator delete OBS bucket tagging

    :param obs_conn_id: The Airflow connection used for OBS credentials.
    :param region: OBS region you want to set bucket tagging
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可删除任意区域的桶标签
    :param bucket_name: This is bucket name you want to delete bucket tagging.
    """

    template_fields: Sequence[str] = ("bucket_name",)

    def __init__(
            self,
            obs_conn_id: str | None = "obs_default",
            region: str | None = None,
            bucket_name: str | None = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.obs_conn_id = obs_conn_id
        self.bucket_name = bucket_name
        self.region = region

    def execute(self, context: Context):
        obs_hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)
        if obs_hook.exist_bucket(self.bucket_name):
            obs_hook.delete_bucket_tagging(bucket_name=self.bucket_name)
        else:
            self.log.warning("OBS Bucket with name: %s doesn't exist on region: %s", self.bucket_name, obs_hook.region)


class OBSCreateObjectOperator(BaseOperator):
    """
    This operator to create object

    This operator returns a python list containing the names of failed upload objects,
    which can be used by 'xcom' in downstream tasks.

    :param obs_conn_id: The Airflow connection used for OBS credentials.
    :param region: OBS region you want to upload an file-like object to bucket
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可将对象上传任意区域的桶中
    :param bucket_name: This is bucket name you want to create
    :param object_key: the OBS path of the object
    :param object_type: 上传对象类型，默认为content
        file
            data参数为待上传文件/文件夹的完整路径，如/aa/bb.txt，或/aa/。
        content
            data参数为使用字符串作为对象的数据源，上传文本到指定桶，
            或者使用包含“read”属性的可读对象作为对象的数据源，以网络流或文件流方式上传数据到指定桶。
    :param data: 待上传数据，结合object_type使用
        如果data是文件夹，则md5会被忽略。
    :param metadata: 上传文件的自定义元数据
    :param md5: 待上传对象数据的MD5值（经过Base64编码），提供给OBS服务端，校验数据完整性。
    :param acl: 上传对象时可指定的预定义访问策略，访问策略如下:
            PRIVATE 私有读写。
            PUBLIC_READ 公共读。
            PUBLIC_READ_WRITE   公共读写。
            BUCKET_OWNER_FULL_CONTROL   桶或对象所有者拥有完全控制权限。
    :param storage_class: 上传对象时可指定的对象的存储类型, 存储类型如下:
            STANDARD    标准存储    标准存储拥有低访问时延和较高的吞吐量，适用于有大量热点对象（平均一个月多次）或小对象（<1MB），且需要频繁访问数据的业务场景。
            WARM    低频访问存储    低频访问存储适用于不频繁访问（平均一年少于12次）但在需要时也要求能够快速访问数据的业务场景。
            COLD    归档存储    归档存储适用于很少访问（平均一年访问一次）数据的业务场景。
    :param expires: 待上传对象的生命周期，单位：天
    :param encryption: 以SSE-KMS方式加密对象，支持的值：kms
    :param key: SSE-KMS方式下加密的主密钥，可为空。
    """

    template_fields: Sequence[str] = ("bucket_name", "object_key", "object_type", "data", "metadata")

    def __init__(
            self,
            object_key: str,
            data: str | object,
            object_type: str | None = 'content',
            region: str | None = None,
            bucket_name: str | None = None,
            obs_conn_id: str | None = "obs_default",
            metadata: dict | None = None,
            md5: str | None = None,
            acl: str | None = None,
            encryption: str | None = None,
            key: str | None = None,
            storage_class: str | None = None,
            expires: int | None = None,
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.region = region
        self.object_key = object_key
        self.object_type = object_type
        self.data = data
        self.bucket_name = bucket_name
        self.obs_conn_id = obs_conn_id
        self.metadata = metadata
        self.headers = {
            'md5': md5,
            'acl': acl,
            'encryption': encryption,
            'key': key,
            'storageClass': storage_class,
            'expires': expires,
        }

    def execute(self, context: Context):
        obs_hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)
        obs_hook.create_object(
            bucket_name=self.bucket_name,
            object_key=self.object_key,
            object_type=self.object_type,
            data=self.data,
            metadata=self.metadata,
            headers=self.headers,
        )


class OBSDownloadObjectOperator(BaseOperator):
    """
    This operator to Download an OBS object

    If load_stream_in_memory is True,
    this operator returns the byte stream of an object,
    which can be used by 'xcom' in downstream tasks.

    :param obs_conn_id: The Airflow connection used for OBS credentials.
    :param region: OBS region
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可下载任意区域的桶中对象到本地
    :param bucket_name: OBS bucket name
    :param object_key: key of the object to download.
    :param download_path: 下载对象的目标路径，包含文件名，如aa/bb.txt.
    :param load_stream_in_memory: 是否将对象的数据流加载到内存。
        默认值为False，如果该值为True，会忽略downloadPath参数，并返回将获取的数据流。
    """
    ui_color = "#3298ff"
    template_fields: Sequence[str] = ("bucket_name", "object_key", "download_path", "load_stream_in_memory")

    def __init__(
            self,
            object_key: str,
            download_path: str | None = None,
            bucket_name: str | None = None,
            load_stream_in_memory: str | None = False,
            region: str | None = None,
            obs_conn_id: str | None = "obs_default",
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.obs_conn_id = obs_conn_id
        self.region = region
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.load_stream_in_memory = load_stream_in_memory
        self.download_path = download_path

    def execute(self, context: Context):
        obs_hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)
        obs_hook.download_file_object(
            bucket_name=self.bucket_name,
            object_key=self.object_key,
            download_path=self.download_path,
            load_stream_in_memory=self.load_stream_in_memory,
        )


class OBSCopyObjectOperator(BaseOperator):
    """
    This operator to copy an OBS object

    :param obs_conn_id: The Airflow connection used for OBS credentials.
    :param region: OBS region
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可复制任意区域的桶中对象
    :param source_object_key: 源对象名。
    :param dest_object_key: 目标对象名
    :param source_bucket_name: 源桶名
    :param dest_bucket_name: 目标桶名
    :param versionId: 源对象版本号
    :param metadata: 目标对象的自定义元数据(需要指定directive为'REPLACE')
    :param directive: 目标对象的属性是否从源对象中复制，支持的值：
        COPY: 默认值，从源对象复制
        REPLACE: 以请求参数中指定的值替换
    :param acl: 复制对象时可指定的预定义访问策略。
        PRIVATE 私有读写。
        PUBLIC_READ 公共读。
        PUBLIC_READ_WRITE   公共读写。
        BUCKET_OWNER_FULL_CONTROL   桶或对象所有者拥有完全控制权限。
    """

    template_fields: Sequence[str] = (
        "source_object_key",
        "dest_object_key",
        "source_bucket_name",
        "dest_bucket_name",
    )

    def __init__(
            self,
            source_object_key: str,
            dest_object_key: str,
            source_bucket_name: str | None = None,
            dest_bucket_name: str | None = None,
            version_id: str | None = None,
            metadata: dict | None = None,
            directive: str | None = 'COPY',
            acl: str | None = None,
            region: str | None = None,
            obs_conn_id: str | None = "obs_default",
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.obs_conn_id = obs_conn_id
        self.region = region
        self.source_object_key = source_object_key
        self.dest_object_key = dest_object_key
        self.source_bucket_name = source_bucket_name
        self.dest_bucket_name = dest_bucket_name
        self.version_id = version_id
        self.headers = {
            'directive': directive,
            'acl': acl,
        }
        self.metadata = metadata

    def execute(self, context: Context):
        obs_hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)
        obs_hook.copy_object(
            source_object_key=self.source_object_key,
            dest_object_key=self.dest_object_key,
            source_bucket_name=self.source_bucket_name,
            dest_bucket_name=self.dest_bucket_name,
            version_id=self.version_id,
            headers=self.headers,
            metadata=self.metadata,
        )


class OBSDeleteObjectOperator(BaseOperator):
    """
    This operator to delete an OBS object

    :param obs_conn_id: The Airflow connection used for OBS credentials.
    :param region: OBS region
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可删除任意区域的桶中对象
    :param bucket_name: OBS bucket name
    :param object_key: object name
    :param version_id: object version id
    """

    template_fields: Sequence[str] = (
        "bucket_name",
        "object_key",
        "version_id",
    )

    def __init__(
            self,
            object_key: str,
            bucket_name: str | None = None,
            version_id: str | None = None,
            region: str | None = None,
            obs_conn_id: str | None = "obs_default",
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.obs_conn_id = obs_conn_id
        self.region = region
        self.object_key = object_key
        self.bucket_name = bucket_name
        self.version_id = version_id

    def execute(self, context: Context):
        obs_hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)
        obs_hook.delete_object(
            object_key=self.object_key,
            bucket_name=self.bucket_name,
            version_id=self.version_id,
        )


class OBSDeleteBatchObjectOperator(BaseOperator):
    """
    This operator to delete OBS objects

    :param obs_conn_id: The Airflow connection used for OBS credentials.
    :param region: OBS region
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可删除任意区域的桶中对象
    :param bucket_name: OBS bucket name
    :param object_list: 待删除的对象列表
        对象非多版本表示方式 ['object_key1', 'object_key2', ...]
        对象存在多版本表示方式[{'object_key': 'test_key', 'version_id': 'test_version'}, ...]
        或者['object_key1', 'object_key2', ...]
    :param quiet: 批量删除对象的响应方式
        False表示详细模式，返回的删除成功和删除失败的所有结果
        True表示简单模式，只返回的删除过程中出错的结果。
    """

    def __init__(
            self,
            object_list: list[str | dict[str, str]],
            bucket_name: str,
            quiet: bool | None = True,
            region: str | None = None,
            obs_conn_id: str | None = "obs_default",
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.obs_conn_id = obs_conn_id
        self.region = region
        self.bucket_name = bucket_name
        self.object_list = object_list
        self.quiet = quiet

    def execute(self, context: Context):
        obs_hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)
        if obs_hook.exist_bucket(self.bucket_name):
            obs_hook.delete_objects(
                bucket_name=self.bucket_name,
                object_list=self.object_list,
                quiet=self.quiet,
            )
        else:
            self.log.warning("OBS Bucket with name: %s doesn't exist on region: %s", self.bucket_name, obs_hook.region)


class OBSMoveObjectOperator(BaseOperator):
    """
    This operator to move OBS object

    :param obs_conn_id: The Airflow connection used for OBS credentials.
    :param region: OBS region
        默认从obs_conn_id对应的connetction中获取
        region为cn-north-1时可移动任意区域的桶中对象
    :param bucket_name: OBS bucket name
    :param source_object_key: 源对象名
    :param dest_object_key: 目标对象名
    """

    template_fields: Sequence[str] = (
        "bucket_name",
        "source_object_key",
        "dest_object_key",
    )

    def __init__(
            self,
            source_object_key: str,
            dest_object_key: str,
            bucket_name: str | None = None,
            region: str | None = None,
            obs_conn_id: str | None = "obs_default",
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_object_key = source_object_key
        self.dest_object_key = dest_object_key
        self.bucket_name = bucket_name
        self.obs_conn_id = obs_conn_id
        self.region = region

    def execute(self, context: Context):
        obs_hook = OBSHook(obs_conn_id=self.obs_conn_id, region=self.region)
        if obs_hook.exist_bucket(self.bucket_name):
            obs_hook.move_object(
                bucket_name=self.bucket_name,
                source_object_key=self.source_object_key,
                dest_object_key=self.dest_object_key
            )
        else:
            self.log.warning("OBS Bucket with name: %s doesn't exist on region: %s", self.bucket_name, obs_hook.region)
