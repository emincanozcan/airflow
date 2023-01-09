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
from __future__ import annotations

from functools import wraps
from inspect import signature
from typing import TYPE_CHECKING, Callable, TypeVar, cast
from urllib.parse import urlsplit

from obs.bucket import BucketClient
from obs import ObsClient, Tag, TagInfo, \
    PutObjectHeader, CopyObjectHeader, \
    DeleteObjectsRequest, Object, SseKmsHeader

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from airflow.models.connection import Connection

T = TypeVar("T", bound=Callable)


def provide_bucket_name(func: T) -> T:
    """
    Function decorator that unifies bucket name and key taken from the key
    in case no bucket name and at least a key has been passed to the function.
    """
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        bound_args = function_signature.bind(*args, **kwargs)
        self = args[0]
        if bound_args.arguments.get("bucket_name") is None and self.obs_conn_id:
            connection = self.get_connection(self.obs_conn_id)
            if connection.schema:
                bound_args.arguments["bucket_name"] = connection.schema

        return func(*bound_args.args, **bound_args.kwargs)

    return cast(T, wrapper)


def unify_bucket_name_and_key(func: T) -> T:
    """
    Function decorator that unifies bucket name and object_key taken from the object_key
    in case no bucket name and at least a object_key has been passed to the function.
    """
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        bound_args = function_signature.bind(*args, **kwargs)

        def get_key() -> str:
            if "object_key" in bound_args.arguments:
                return "object_key"
            raise ValueError("Missing object_key parameter!")

        key_name = get_key()
        if "bucket_name" not in bound_args.arguments or bound_args.arguments["bucket_name"] is None:
            bound_args.arguments["bucket_name"], bound_args.arguments["object_key"] = OBSHook.parse_obs_url(
                bound_args.arguments[key_name]
            )

        return func(*bound_args.args, **bound_args.kwargs)

    return cast(T, wrapper)


class OBSHook(BaseHook):
    """Interact with Huawei Cloud OBS, using the obs library."""

    conn_name_attr = "huaweicloud_conn_id"
    default_conn_name = "obs_default"
    conn_type = "obs"
    hook_name = "OBS"

    def __init__(self, region: str | None = None, obs_conn_id="obs_default", *args, **kwargs) -> None:
        self.obs_conn_id = obs_conn_id
        self.obs_conn = self.get_connection(obs_conn_id)
        self.region = self.get_default_region() if region is None else region
        super().__init__(*args, **kwargs)

    def get_conn(self) -> Connection:
        """Returns connection for the hook."""
        return self.obs_conn

    @staticmethod
    def parse_obs_url(obsurl: str) -> tuple:
        """
        Parses the OBS Url into a bucket name and object key.

        :param obsurl: The OBS Url to parse.
        :return: the parsed bucket name and object key
        """
        parsed_url = urlsplit(obsurl)

        if not parsed_url.netloc:
            raise AirflowException(f'Please provide a bucket_name instead of "{obsurl}"')

        bucket_name = parsed_url.netloc.split('.', 1)[0]
        object_key = parsed_url.path.lstrip("/")

        return bucket_name, object_key

    @staticmethod
    def get_obs_bucket_object_key(
        bucket_name: str | None, object_key: str, bucket_param_name: str, object_key_param_name: str
    ) -> tuple[str, str]:
        """
        Get the OBS bucket name and object key from either:
            - bucket name and object key. Return the info as it is after checking `object_key` is a relative path
            - object key. Must be a full obs:// url

        :param bucket_name: The OBS bucket name
        :param object_key: The OBS object key
        :param bucket_param_name: The parameter name containing the bucket name
        :param object_key_param_name: The parameter name containing the object key name
        :return: the parsed bucket name and object key
        """
        if bucket_name is None:
            return OBSHook.parse_obs_url(object_key)

        parsed_url = urlsplit(object_key)
        if parsed_url.scheme != "" or parsed_url.netloc != "":
            raise TypeError(
                f"If `{bucket_param_name}` is provided, {object_key_param_name} should be a relative path "
                "from root level, rather than a full obs:// url"
            )

        return bucket_name, object_key

    def get_obs_client(self) -> ObsClient:
        """
        获取OBS客户端，用于管理桶和对象等OBS服务上的资源
        Returns a obs Client

        :param region: the endpoint of region.
        """
        auth = self.get_credential()
        access_key_id, secret_access_key = auth
        server = f'https://obs.{self.region}.myhuaweicloud.com'
        return ObsClient(access_key_id=access_key_id, secret_access_key=secret_access_key, server=server)

    @provide_bucket_name
    def get_bucket_client(self, bucket_name: str | None = None) -> BucketClient:
        """
        Returns a OBS bucket client object

        :param bucket_name: the name of the bucket
        :return: the bucket object to the bucket name.
        """
        return self.get_obs_client().bucketClient(bucketName=bucket_name)

    @provide_bucket_name
    def create_bucket(self, bucket_name: str | None = None) -> None:
        """
        创建桶，创建桶的区域region应与客户端的区域region相同

        :param bucket_name: The name of the bucket.
        :param region: The name of the region in which to create the bucket.
        """

        try:
            resp = self.get_bucket_client(bucket_name).createBucket(location=self.region)
            if resp.status < 300:
                self.log.info(f'Created OBS bucket with name: {bucket_name}')
            else:
                self.log.error(f'创建桶错误信息：{resp.errorCode}\t{resp.errorMessage}')
        except Exception as e:
            raise AirflowException(f"Errors when create bucket: {bucket_name}({e})")

    @provide_bucket_name
    def list_bucket(self) -> list[str]:
        """
        获取桶名列表，region为cn-north-1时获取所有桶名，
        其它region只获取对应区域的桶名
        """
        try:
            resp = self.get_obs_client().listBuckets()
            if resp.status < 300:
                return [bucket.name for bucket in resp.body.buckets]
            else:
                self.log.error(f'查询桶列表错误信息：{resp.errorCode}\t{resp.errorMessage}')
        except Exception as e:
            raise AirflowException(f"Errors when list bucket({e})")

    @provide_bucket_name
    def exist_bucket(self, bucket_name) -> bool:
        """
        判断桶是否存在

        :param bucket_name: The name of the bucket
        """
        resp = self.get_bucket_client(bucket_name).headBucket()

        if resp.status < 300:
            return True
        elif resp.status == 404:
            self.log.info(f'Bucket {bucket_name} does not exist')
        elif resp.status == 403:
            self.log.error(f'{resp.errorCode}\t{resp.errorMessage}')
        return False

    # TODO: 非空桶无法直接删除
    @provide_bucket_name
    def delete_bucket(
        self,
        bucket_name: str | None = None,
    ) -> None:
        """
        Delete bucket from OBS

        :param bucket_name: the name of the bucket
        """
        try:
            resp = self.get_bucket_client(bucket_name).deleteBucket()
            if resp.status < 300:
                self.log.info(f'Deleted OBS bucket: {bucket_name}')
            else:
                self.log.error(f'删除桶错误信息：{resp.errorCode}\t{resp.errorMessage}')
        except Exception as e:
            raise AirflowException(f"Errors when deleting: {bucket_name}({e})")

    @provide_bucket_name
    def get_bucket_tagging(self, bucket_name: str | None = None, ) -> list[dict[str, str]]:
        """
        Get bucket tagging from OBS

        :param bucket_name: the name of the bucket
        """
        try:
            resp = self.get_bucket_client(bucket_name).getBucketTagging()
            if resp.status < 300:
                return resp.body.tagSet
            else:
                self.log.error(f'获取桶标签错误信息：{resp.errorCode}\t{resp.errorMessage}')
                return []
        except Exception as e:
            raise AirflowException(f"Errors when getting the bucket tagging of {bucket_name}({e})")

    @provide_bucket_name
    def set_bucket_tagging(
        self,
        tag_info: list[dict[str, str]] | None = None,
        bucket_name: str | None = None,
    ) -> None:
        """
        Set bucket tagging from OBS

        :param bucket_name: the name of the bucket
        :param tag_info: 桶标签配置
        """

        if not tag_info:
            self.log.warning('No "tag_info" information was passed')
            return

        tags = [Tag(key=tag['key'], value=tag['value']) for tag in tag_info]
        tag_info = TagInfo(tags)

        try:
            resp = self.get_bucket_client(bucket_name).setBucketTagging(tagInfo=tag_info)
            if resp.status < 300:
                self.log.info('设置桶标签成功')
            else:
                self.log.error(f'设置桶标签错误信息：{resp.errorCode}\t{resp.errorMessage}')
        except Exception as e:
            raise AirflowException(f"Errors when setting the bucket tagging of {bucket_name}({e})")

    @provide_bucket_name
    def delete_bucket_tagging(
        self,
        bucket_name: str | None = None,
    ) -> None:
        """
        Delete bucket tagging from OBS

        :param bucket_name: the name of the bucket
        """
        try:
            resp = self.get_bucket_client(bucket_name).deleteBucketTagging()
            if resp.status < 300:
                self.log.info('删除桶标签成功')
            else:
                self.log.error(f'删除桶标签错误信息：{resp.errorCode}\t{resp.errorMessage}')
        except Exception as e:
            raise AirflowException(f"Errors when deleting the bucket tagging of {bucket_name}({e})")

    def exist_object(
        self,
        object_key: str,
        bucket_name: str | None = None,
    ) -> bool:
        """
        判断桶对象是否存在

        :param bucket_name: 桶名
        :param object_key: 桶对象名
        """

        bucket_name, object_key = OBSHook.get_obs_bucket_object_key(
            bucket_name, object_key, "bucket_name", "object_key"
        )
        object_list = self.list_object(bucket_name=bucket_name, prefix=object_key, max_keys=1)
        return object_key in object_list

    @provide_bucket_name
    def list_object(
        self,
        bucket_name: str | None = None,
        prefix: str | None = None,
        marker: str | None = None,
        max_keys: int | None = None,
        is_truncated: bool | None = False,
    ) -> list | None:
        """
        列举桶内对象(全局)
        可以列举指定区域内的桶对象(cn-north-1除外,所有桶有效)

        :param bucket_name: This is bucket name you want to list all objects
        :param prefix: 限定返回的对象名必须带有prefix前缀。
        :param marker: 列举桶内对象列表时，指定一个标识符，从该标识符以后按字典顺序返回对象列表。
        :param max_keys: 列举对象的最大数目，取值范围为1~1000，当超出范围时，按照默认的1000进行处理。
        :param is_truncated: 是否开启分页查询，列举符合条件的所有对象
        """
        bucket_client = self.get_bucket_client(bucket_name)
        resp = self._list_object(
            bucket_client,
            prefix=prefix,
            marker=marker,
            max_keys=max_keys,
        )
        if resp.status < 300:
            object_list = [content.key for content in resp.body.contents]
            if not is_truncated:
                return object_list
        else:
            self.log.error(f'列举桶对象错误信息：{resp.errorCode}\t{resp.errorMessage}')
            return None

        object_lists = [object_list]

        while resp.body.is_truncated:
            resp = self._list_object(
                bucket_client,
                prefix=prefix,
                marker=resp.body.next_marker,
                max_keys=max_keys,
            )
            if resp.status < 300:
                object_lists.append([content.key for content in resp.body.contents])
            else:
                self.log.error(f'列举桶对象错误信息：{resp.errorCode}\t{resp.errorMessage}')
                return None
        return object_lists

    @staticmethod
    def _list_object(self, bucket_client, **kwargs):
        return bucket_client.listObjects(**kwargs)

    @provide_bucket_name
    @unify_bucket_name_and_key
    def create_object(
        self,
        object_key: str,
        data: str | object,
        object_type: str | None = 'content',
        bucket_name: str | None = None,
        metadata: dict | None = None,
        headers: dict | None = None,
    ) -> None | list:
        """
        上传对象到指定OBS桶

        :param bucket_name: the name of the bucket
        :param object_key: the OBS path of the object
        :param object_type: 上传对象类型，默认为content
            file
                待上传文件/文件夹的完整路径，如/aa/bb.txt，或/aa/。
            content
                使用字符串作为对象的数据源，上传文本到指定桶，
                或者使用包含“read”属性的可读对象作为对象的数据源，以网络流或文件流方式上传数据到指定桶。
        :param data: 待上传对象
            如果data是文件夹类型，则md5会被忽略。
        :param metadata: 上传对象的自定义元数据。
        :param headers: 上传对象的附加头域。
        """
        if object_type not in ['content', 'file']:
            raise AirflowException(f"invalid object_type(choices 'content', 'file')")
        try:
            sse_header = self._get_encryption_header(
                encryption=headers.pop('encryption'),
                key=headers.pop('key')
            )
            headers = PutObjectHeader(sseHeader=sse_header, **headers)
            if object_type == 'content':
                resp = self.get_bucket_client(bucket_name).putContent(
                    objectKey=object_key,
                    content=data,
                    metadata=metadata,
                    headers=headers,
                )
                if getattr(data, 'read', None):
                    data.close()
            else:
                resp = self.get_bucket_client(bucket_name).putFile(
                    objectKey=object_key,
                    file_path=data,
                    metadata=metadata,
                    headers=headers,
                )

            if isinstance(resp, list):
                err_object = [i[0] for i in self.flatten(resp) if i[1]['status'] >= 300]
                if err_object:
                    self.log.error(f'上传失败的对象: {err_object}')
                    return err_object
                return None

            if resp.status < 300:
                self.log.info(f'对象上传成功，对象链接：{resp.body.objectUrl}')
                return resp.body.objectUrl
            else:
                self.log.error(f'对象上传错误信息：{resp.errorCode}\t{resp.errorMessage}')
            return None

        except Exception as e:
            if object_type == 'content' and getattr(data, 'read', None):
                data.close()
            raise AirflowException(f"Errors when create object({e})")

    @staticmethod
    def _get_encryption_header(self, encryption, key):
        if encryption == 'kms':
            return SseKmsHeader(encryption=encryption, key=key)

    def flatten(self, sequence):
        for item in sequence:
            if isinstance(item[1], list):
                for subitem in self.flatten(item[1]):
                    yield subitem
            else:
                yield item

    @provide_bucket_name
    @unify_bucket_name_and_key
    def download_file_object(
        self,
        object_key: str,
        download_path: str | None = None,
        bucket_name: str | None = None,
        load_stream_in_memory: str | None = False,
    ) -> str | None:
        """
        Download file from OBS

        :param bucket_name: the name of the bucket
        :param object_key: key of the file-like object to download.
        :param download_path: 下载对象的目标路径，包含文件名，如aa/bb.txt
            当选择当前目录时，路径格式需要指定当前目录，如./xxx，不可仅用文件名
        :param load_stream_in_memory: 是否将对象的数据流加载到内存。
            默认值为False，如果该值为True，会忽略downloadPath参数，并将获取的数据流直接加载到内存。
        """
        try:
            resp = self.get_bucket_client(bucket_name).getObject(object_key, download_path,
                                                                 loadStreamInMemory=load_stream_in_memory)
            if resp.status < 300:
                if load_stream_in_memory:
                    return resp.body.buffer
                self.log.info('对象下载成功')
            else:
                self.log.error(f'对象下载错误信息：{resp.errorCode}\t{resp.errorMessage}')
        except Exception as e:
            raise AirflowException(f"Errors when download: {object_key}({e})")

    def copy_object(
        self,
        source_object_key: str,
        dest_object_key: str,
        source_bucket_name: str | None = None,
        dest_bucket_name: str | None = None,
        version_id: str | None = None,
        headers: dict | None = None,
        metadata: dict | None = None,
    ) -> None:
        """
        Download file from OBS

        :param source_object_key: 源对象名。
        :param dest_object_key: 目标对象名
        :param source_bucket_name: 源桶名
        :param dest_bucket_name: 目标桶名
        :param version_id: 源对象版本号
        :param metadata: 目标对象的自定义元数据(需要指定headers中的directive为'REPLACE')
        :param headers: 复制对象的附加头域
        """
        try:
            headers = CopyObjectHeader(**headers)

            dest_bucket_name, dest_object_key = self.get_obs_bucket_object_key(
                dest_bucket_name, dest_object_key, "dest_bucket_name", "dest_object_key"
            )

            source_bucket_name, source_object_key = self.get_obs_bucket_object_key(
                source_bucket_name, source_object_key, "source_bucket_name", "source_object_key"
            )

            resp = self.get_obs_client().copyObject(
                sourceObjectKey=source_object_key,
                destObjectKey=dest_object_key,
                sourceBucketName=source_bucket_name,
                destBucketName=dest_bucket_name,
                versionId=version_id,
                headers=headers,
                metadata=metadata,
            )
            if resp.status < 300:
                self.log.info('对象复制成功')
            else:
                self.log.error(f'对象复制错误信息：{resp.errorCode}\t{resp.errorMessage}')
        except Exception as e:
            raise AirflowException(f"Errors when copying: {source_object_key}({e})")

    @provide_bucket_name
    @unify_bucket_name_and_key
    def delete_object(
        self,
        object_key: str,
        version_id: str | None = None,
        bucket_name: str | None = None,
    ) -> None:
        """
        Delete object from OBS

        :param bucket_name: OBS bucket name
        :param object_key: object name
        :param version_id: object version id
        """
        try:
            bucket_name, object_key = self.get_obs_bucket_object_key(
                bucket_name, object_key, "bucket_name", "object_key"
            )
            resp = self.get_bucket_client(bucket_name).deleteObject(
                objectKey=object_key,
                versionId=version_id,
            )
            if resp.status < 300:
                self.log.info('对象删除成功')
            else:
                self.log.error(f'对象删除错误信息：{resp.errorCode}\t{resp.errorMessage}')
        except Exception as e:
            raise AirflowException(f"Errors when deleting: {object_key}({e})")

    def delete_objects(
        self,
        bucket_name: str,
        object_list: list,
        quiet: bool,
    ) -> None:
        """
        Delete objects from OBS

        :param bucket_name: OBS bucket name
        :param object_list: 待删除的对象列表
            对象非多版本表示方式 ['object_key1', 'object_key2', ...]
            对象存在多版本表示方式[{'object_key': 'test_key', 'version_id': 'test_version'}, ...]
            或者['object_key1', 'object_key2', ...]
        :param quiet: 批量删除对象的响应方式
            False表示详细模式，返回的删除成功和删除失败的所有结果
            True表示简单模式，只返回的删除过程中出错的结果。
        """
        try:
            if not object_list:
                self.log.warning('对象列表为空')
                return
            if len(object_list) > 1000:
                self.log.warning('批量删除对象一次能接收最大对象数目为1000个, 已超出限制')
                return
            if isinstance(object_list[0], dict):
                objects = [
                    Object(key=obj.get('object_key', None), versionId=obj.get('version_id', None)) for
                    obj in object_list]
            else:
                objects = [Object(key=obj) for obj in object_list]
            delete_objects_request = DeleteObjectsRequest(objects=objects, quiet=quiet)
            resp = self.get_bucket_client(bucket_name).deleteObjects(delete_objects_request)
            if resp.status < 300:
                self.log.info('对象批量删除成功')
            else:
                self.log.error(f'对象批量删除错误信息：{resp.errorCode}\t{resp.errorMessage}')
        except Exception as e:
            raise AirflowException(f"Errors when deleting: {object_list}({e})")

    @provide_bucket_name
    def move_object(
        self,
        source_object_key: str,
        dest_object_key: str,
        bucket_name: str | None = None,
    ) -> None:
        """
        Move object from OBS

        :param bucket_name: OBS bucket name
        :param source_object_key: 源对象名
        :param dest_object_key: 目标对象名
        """
        try:
            bucket = self.get_bucket_client(bucket_name)
            copy_resp = bucket.copyObject(
                sourceObjectKey=source_object_key,
                sourceBucketName=bucket_name,
                destObjectKey=dest_object_key,
            )
            if copy_resp.status < 300:
                delete_resp = bucket.deleteObject(
                    objectKey=source_object_key,
                )
                if delete_resp.status < 300:
                    self.log.info('对象移动成功')
                else:
                    self.log.error(f'对象移动错误信息：{delete_resp.errorCode}\t{delete_resp.errorMessage}')
            else:
                self.log.error(f'对象移动错误信息：{copy_resp.errorCode}\t{copy_resp.errorMessage}')
        except Exception as e:
            raise AirflowException(f"Errors when Moving: {source_object_key}({e})")

    def get_credential(self) -> tuple:
        """
        从connection的extra_config选项中获取用户认证信息
        """
        extra_config = self.obs_conn.extra_dejson
        auth_type = extra_config.get("auth_type", None)
        if not auth_type:
            raise Exception("No auth_type specified in extra_config. ")

        if auth_type != "AK":
            raise Exception(f"Unsupported auth_type: {auth_type}")
        access_key_id = extra_config.get("access_key_id", None)
        access_key_secret = extra_config.get("access_key_secret", None)
        if not access_key_id:
            raise Exception(f"No access_key_id is specified for connection: {self.obs_conn_id}")

        if not access_key_secret:
            raise Exception(f"No access_key_secret is specified for connection: {self.obs_conn_id}")

        return access_key_id, access_key_secret

    def get_default_region(self) -> str | None:
        """
        从connection的extra_config选项中获取区域信息
        """
        extra_config = self.obs_conn.extra_dejson
        auth_type = extra_config.get("auth_type", None)
        if not auth_type:
            raise Exception("No auth_type specified in extra_config. ")

        if auth_type != "AK":
            raise Exception(f"Unsupported auth_type: {auth_type}")

        default_region = extra_config.get("region", None)
        if not default_region:
            raise Exception(f"No region is specified for connection: {self.obs_conn_id}")
        return default_region
