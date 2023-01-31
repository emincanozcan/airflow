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

import json
from functools import wraps
from inspect import signature
from typing import TYPE_CHECKING, Callable, TypeVar, cast, Any
from urllib.parse import urlsplit

from obs.bucket import BucketClient
from obs import (ObsClient, Tag, TagInfo, Object,
                 PutObjectHeader, CopyObjectHeader,
                 DeleteObjectsRequest, SseKmsHeader)

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from airflow.models.connection import Connection

T = TypeVar("T", bound=Callable)


def provide_bucket_name(func: T) -> T:
    """
    Function decorator that provides a bucket name taken from the connection
    in case no bucket name has been passed to the function.
    """
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        bound_args = function_signature.bind(*args, **kwargs)
        self = args[0]
        if bound_args.arguments.get("bucket_name") is None and self.huaweicloud_conn_id:
            connection = self.get_connection(self.huaweicloud_conn_id)
            if connection.schema:
                bound_args.arguments["bucket_name"] = connection.schema

        return func(*bound_args.args, **bound_args.kwargs)

    return cast(T, wrapper)


def unify_bucket_name_and_key(func: T) -> T:
    """
    Function decorator that unifies bucket name and object_key taken from the object_key
    in case no bucket name and at least an object_key has been passed to the function.
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
            bound_args.arguments["bucket_name"], bound_args.arguments["object_key"] = ObsHook.parse_obs_url(
                bound_args.arguments[key_name]
            )

        return func(*bound_args.args, **bound_args.kwargs)

    return cast(T, wrapper)


def get_err_info(resp):
    return json.dumps({
        "status": resp.status,
        "reason": resp.reason,
        "errorCode": resp.errorCode,
        "errorMessage": resp.errorMessage
    })


class ObsHook(BaseHook):
    """Interact with Huawei Cloud OBS, using the obs library."""

    conn_name_attr = "huaweicloud_conn_id"
    default_conn_name = "huaweicloud_default"
    conn_type = "huaweicloud"
    hook_name = "OBS"

    def __init__(self, region: str | None = None, huaweicloud_conn_id="huaweicloud_default", *args,
                 **kwargs) -> None:
        self.huaweicloud_conn_id = huaweicloud_conn_id
        self.obs_conn = self.get_connection(huaweicloud_conn_id)
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
        :return: the parsed bucket name and object key.
        """
        parsed_url = urlsplit(obsurl)
        if not parsed_url.netloc:
            raise AirflowException(f"Please provide a bucket_name instead of '{obsurl}'.")

        bucket_name = parsed_url.netloc.split('.', 1)[0]
        object_key = parsed_url.path.lstrip("/")

        return bucket_name, object_key

    @staticmethod
    def get_obs_bucket_object_key(
        bucket_name: str | None, object_key: str, bucket_param_name: str, object_key_param_name: str
    ) -> tuple[str, str]:
        """
        Get the OBS bucket name and object key from either:
            - bucket name and object key. Return the info as it is after checking `object_key` is a relative path.
            - object key. Must be a full obs:// url

        :param bucket_name: The OBS bucket name.
        :param object_key: The OBS object key.
        :param bucket_param_name: The parameter name containing the bucket name.
        :param object_key_param_name: The parameter name containing the object key name.
        :return: the parsed bucket name and object key.
        """
        if bucket_name is None:
            return ObsHook.parse_obs_url(object_key)

        parsed_url = urlsplit(object_key)
        if parsed_url.scheme != "" or parsed_url.netloc != "":
            raise TypeError(
                f"If `{bucket_param_name}` is provided, {object_key_param_name} should be a relative path "
                "from root level, rather than a full obs:// url."
            )

        return bucket_name, object_key

    def get_obs_client(self) -> ObsClient:
        """
        Gets an OBS client to manage resources on OBS services such as buckets and objects.
        Returns an OBS Client.

        :param region: the endpoint of region.
        """
        auth = self.get_credential()
        access_key_id, secret_access_key = auth
        server = f"https://obs.{self.region}.myhuaweicloud.com"
        return ObsClient(access_key_id=access_key_id, secret_access_key=secret_access_key, server=server)

    @provide_bucket_name
    def get_bucket_client(self, bucket_name: str | None = None) -> BucketClient:
        """
        Returns an OBS bucket client object.

        :param bucket_name: the name of the bucket.
        :return: the bucket object to the bucket name.
        """
        return self.get_obs_client().bucketClient(bucket_name)

    @provide_bucket_name
    def create_bucket(self, bucket_name: str | None = None) -> None:
        """
        Create a bucket.

        :param bucket_name: The name of the bucket.
        :param region: The name of the region in which to create the bucket.
        """

        try:
            resp = self.get_bucket_client(bucket_name).createBucket(location=self.region)
            if resp.status < 300:
                self.log.info(f"Created OBS bucket with name: {bucket_name}.")
            else:
                self.log.error(f"Error message when creating a bucket: {get_err_info(resp)}.")
        except Exception as e:
            raise AirflowException(f"Errors when create bucket {bucket_name}({e}).")

    def list_bucket(self) -> list[str] | None:
        """
        Gets a list of OBS bucket names.
        If the region is cn-north-1, obtain the bucket names of all regions,
        other regions only obtain the bucket names of the corresponding regions.
        """
        try:
            resp = self.get_obs_client().listBuckets()
            if resp.status < 300:
                return [bucket.name for bucket in resp.body.buckets]
            else:
                self.log.error(f'查询桶列表错误信息：{get_err_info(resp)}')
        except Exception as e:
            raise AirflowException(f"Errors when list bucket({e}).")

    @provide_bucket_name
    def exist_bucket(self, bucket_name) -> bool:
        """
        Check whether the bucket exists.

        :param bucket_name: The name of the bucket.
        """
        resp = self.get_bucket_client(bucket_name).headBucket()

        if resp.status < 300:
            return True
        elif resp.status == 404:
            self.log.info(f"Bucket {bucket_name} does not exist.")
        elif resp.status == 403:
            self.log.error(f"{get_err_info(resp)}")
        return False

    @provide_bucket_name
    def delete_bucket(
        self,
        bucket_name: str | None = None,
    ) -> None:
        """
        Delete bucket from OBS.
        Non-empty buckets cannot be deleted directly.

        :param bucket_name: the name of the bucket.
        """
        try:
            resp = self.get_bucket_client(bucket_name).deleteBucket()
            if resp.status < 300:
                self.log.info(f"Deleted OBS bucket: {bucket_name}.")
            else:
                self.log.error(f"Error message when deleting a bucket: {get_err_info(resp)}.")
        except Exception as e:
            raise AirflowException(f"Errors when deleting {bucket_name}({e})")

    @provide_bucket_name
    def get_bucket_tagging(self, bucket_name: str | None = None, ) -> list[dict[str, str]]:
        """
        Get bucket tagging from OBS.

        :param bucket_name: the name of the bucket.
        """
        try:
            resp = self.get_bucket_client(bucket_name).getBucketTagging()
            if resp.status < 300:
                return resp.body.tagSet
            else:
                self.log.error(f"Error message when obtaining the bucket label: {get_err_info(resp)}.")
        except Exception as e:
            raise AirflowException(f"Errors when getting the bucket tagging of {bucket_name}({e}).")

    @provide_bucket_name
    def set_bucket_tagging(
        self,
        tag_info: dict[str, str] | None = None,
        bucket_name: str | None = None,
    ) -> None:
        """
        Set bucket tagging from OBS.

        :param bucket_name: the name of the bucket.
        :param tag_info: bucket tagging information.
        """

        if not tag_info:
            self.log.warning("No 'tag_info' information was passed.")
            return
        tags = [Tag(key=tag, value=str(tag_info[tag])) for tag in tag_info]
        tag_info = TagInfo(tags)

        try:
            resp = self.get_bucket_client(bucket_name).setBucketTagging(tagInfo=tag_info)
            if resp.status < 300:
                self.log.info("Setting the bucket tagging succeeded.")
            else:
                self.log.error(f'Error message when setting the bucket tagging: {get_err_info(resp)}.')
        except Exception as e:
            raise AirflowException(f"Errors when setting the bucket tagging of {bucket_name}({e}).")

    @provide_bucket_name
    def delete_bucket_tagging(
        self,
        bucket_name: str | None = None,
    ) -> None:
        """
        Delete bucket tagging from OBS.

        :param bucket_name: the name of the bucket.
        """
        try:
            resp = self.get_bucket_client(bucket_name).deleteBucketTagging()
            if resp.status < 300:
                self.log.info("Deleting the bucket label succeeded.")
            else:
                self.log.error(f"Error message when Deleting the bucket tagging: {get_err_info(resp)}.")
        except Exception as e:
            raise AirflowException(f"Errors when deleting the bucket tagging of {bucket_name}({e}).")

    def exist_object(
        self,
        object_key: str,
        bucket_name: str | None = None,
    ) -> bool:
        """
        Check whether the object in the bucket exists

        :param bucket_name: bucket name.
        :param object_key: object key.
        """

        bucket_name, object_key = ObsHook.get_obs_bucket_object_key(
            bucket_name, object_key, "bucket_name", "object_key"
        )
        resp = self.get_bucket_client(bucket_name).listObjects(prefix=object_key, max_keys=1)
        object_list = None
        if resp.status < 300:
            object_list = [content.key for content in resp.body.contents]
        else:
            self.log.error(f"Error message when checking the object: {get_err_info(resp)}")
        if not object_list:
            return False
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
        Lists the objects of a bucket in the specified region.
        If the region is cn-north-1, you can list the objects of a bucket in any region

        :param bucket_name: This is bucket name you want to list all objects.
        :param prefix: Name prefix that the objects to be listed must contain.
        :param marker: Object name to start with when listing objects in a bucket.
            All objects are listed in the lexicographical order.
        :param max_keys: Maximum number of objects returned in the response. The value ranges from 1 to 1000.
            If the value is not in this range, 1000 is returned by default.
        :param is_truncated: Whether to enable paging query and list all objects that meet the conditions.
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
            self.log.error(f"Error message when listing the objects: {get_err_info(resp)}.")
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
                self.log.error(f"Error message when listing the objects: {get_err_info(resp)}.")
                return None
        return object_lists

    @staticmethod
    def _list_object(bucket_client, **kwargs):
        return bucket_client.listObjects(**kwargs)

    @provide_bucket_name
    @unify_bucket_name_and_key
    def create_object(
        self,
        object_key: str,
        data: str | object,
        object_type: str | None = "content",
        bucket_name: str | None = None,
        metadata: dict | None = None,
        headers: dict | None = None,
    ) -> None | list:
        """
        Uploads an object to the specified bucket.

        :param bucket_name: The name of the bucket.
        :param object_key: Object name or the name of the uploaded file.
        :param object_type: The type of the object，default is content.
            - file
                Full path of the file/folder to be uploaded, for example, /aa/bb.txt, or /aa/.
            - content
                Upload text to the specified bucket using a string as the data source of the object,
                or upload data to the specified bucket as a network stream or file stream using a
                readable object with a "read" attribute as the data source of the object.
        :param data: Object to be uploaded.
            If data is a folder type, the md5 parameter is ignored.
        :param metadata: Upload custom metadata for the object.
        :param headers: The additional header field of the uploaded object.
        """
        if object_type not in ["content", "file"]:
            raise AirflowException(f"invalid object_type(choices 'content', 'file')")
        try:
            from _io import BufferedReader
            headers = headers if headers else {}
            sse_header = self._get_encryption_header(
                encryption=headers.pop("encryption", None),
                key=headers.pop("key", None)
            ) if headers else None
            headers = PutObjectHeader(sseHeader=sse_header, **headers)
            if object_type == "content":
                resp = self.get_bucket_client(bucket_name).putContent(
                    objectKey=object_key,
                    content=data,
                    metadata=metadata,
                    headers=headers,
                )
                if getattr(data, "read", None):
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
                    self.log.error(f"List of objects that failed to upload: {err_object}.")
                    return err_object
                return None

            if resp.status < 300:
                self.log.info(f"Object uploaded successfully, the url of object is {resp.body.objectUrl}.")
                return None
            else:
                self.log.error(f"Error message when uploading the object: {get_err_info(resp)}.")
            return None

        except Exception as e:
            if object_type == 'content' and getattr(data, "read", None):
                data.close()
            raise AirflowException(f"Errors when create object({e}).")

    @staticmethod
    def _get_encryption_header(encryption, key):
        if encryption == "kms":
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
    def get_object(
        self,
        object_key: str,
        download_path: str | None = None,
        bucket_name: str | None = None,
        load_stream_in_memory: bool | None = False,
    ) -> str | None:
        """
        Downloads an object in a specified bucket.

        :param bucket_name: The name of the bucket.
        :param object_key: Object name or the name of the file to be downloaded.
        :param download_path: The target path to which the object is downloaded, including the file name,
            for example, aa/bb.txt.When selecting the current directory,
            the path format must specify the current directory, for example, ./xxx.
        :param load_stream_in_memory: Whether to load the data stream of the object to the memory.
            The default value is False. If the value is True, the downloadPath parameter will be ineffective
            and the obtained data stream will be directly loaded to the memory.
        """
        try:
            resp = self.get_bucket_client(bucket_name).getObject(
                objectKey=object_key,
                download_path=download_path,
                loadStreamInMemory=load_stream_in_memory
            )
            if resp.status < 300:
                if load_stream_in_memory:
                    self.log.info('The object was converted to a string type and loaded into Xcom.')
                    return str(resp.body.buffer, 'utf-8')
                self.log.info('Object download succeeded.')
            else:
                self.log.error(f"Error message when getting the object: {get_err_info(resp)}.")
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
         Creates a copy for an object in a specified bucket.

        :param source_object_key: Source object name.
        :param dest_object_key: Target object name.
        :param source_bucket_name: Source bucket name.
        :param dest_bucket_name: Target bucket name.
        :param version_id: Source object version ID.
        :param metadata: Customized metadata of the target object,
            directive in headers needs to be specified as 'REPLACE'.
        :param headers: Additional header of the request for copying an object.
        """
        try:
            headers = CopyObjectHeader(**headers) if headers else None

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
                self.log.info("Object replication succeeded")
            else:
                self.log.error(f"Error message when copying object: {get_err_info(resp)}.")
        except Exception as e:
            raise AirflowException(f"Errors when copying: {source_object_key}({e}).")

    @provide_bucket_name
    @unify_bucket_name_and_key
    def delete_object(
        self,
        object_key: str,
        version_id: str | None = None,
        bucket_name: str | None = None,
    ) -> None:
        """
        Deletes an object from bucket.

        :param bucket_name: OBS bucket name.
        :param object_key: object name.
        :param version_id: object version id.
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
                self.log.info("Object deleted successfully")
            else:
                self.log.error(f"Error message when deleting object: {get_err_info(resp)}.")
        except Exception as e:
            raise AirflowException(f"Errors when deleting: {object_key}({e})")

    def delete_objects(
        self,
        bucket_name: str,
        object_list: list,
        quiet: bool | None = True,
    ) -> None:
        """
        Deletes objects from a specified bucket in a batch.

        :param bucket_name: OBS bucket name.
        :param object_list: List of objects to be deleted.
        :param quiet: Response mode of a batch deletion request.
            If this field is set to False, objects involved in the deletion will be returned.
            If this field is set to True, only objects failed to be deleted will be returned.
        """
        try:
            if not object_list:
                self.log.warning("The object list is empty.")
                return
            if len(object_list) > 1000:
                self.log.warning(f"A maximum of 1000 objects can be deleted in a batch, "
                                 f"{len(object_list)} is out of limit")
                return
            if isinstance(object_list[0], dict):
                objects = [
                    Object(key=obj.get("object_key", None), versionId=obj.get("version_id", None)) for
                    obj in object_list]
            else:
                objects = [Object(key=obj) for obj in object_list]
            delete_objects_request = DeleteObjectsRequest(objects=objects, quiet=quiet)
            resp = self.get_bucket_client(bucket_name).deleteObjects(delete_objects_request)
            if resp.status < 300:
                self.log.info('Succeeded in deleting objects in batches.')
            else:
                self.log.error(f"Error message when deleting batch objects: {get_err_info(resp)}.")
        except Exception as e:
            raise AirflowException(f"Errors when deleting: {object_list}({e}).")

    def move_object(
        self,
        source_object_key: str,
        dest_object_key: str,
        dest_bucket_name: str,
        source_bucket_name: str | None = None,
    ) -> None:
        """
        Creates a move for an object in a specified bucket.

        :param source_object_key: Source object name.
        :param dest_object_key: Target object name.
        :param source_bucket_name: Source bucket name.
        :param dest_bucket_name: Target bucket name.
        """
        try:

            obs_client = self.get_obs_client()
            copy_resp = obs_client.copyObject(
                sourceObjectKey=source_object_key,
                sourceBucketName=source_bucket_name,
                destBucketName=dest_bucket_name,
                destObjectKey=dest_object_key,
            )
            if copy_resp.status < 300:
                delete_resp = obs_client.deleteObject(
                    objectKey=source_object_key,
                    bucketName=source_bucket_name
                )
                if delete_resp.status < 300:
                    self.log.info('Object moved successfully')
                else:
                    obs_client.deleteObject(
                        objectKey=dest_object_key,
                        bucketName=dest_bucket_name
                    )
                    self.log.error(f"Error message when moving objects: {get_err_info(delete_resp)}.")
            else:
                self.log.error(f"Error message when moving objects: {get_err_info(copy_resp)}.")
        except Exception as e:
            raise AirflowException(f"Errors when Moving: {source_object_key}({e}).")

    def get_credential(self) -> tuple:
        """
        Gets user authentication information from connection.
        """
        access_key_id = self.obs_conn.login
        access_key_secret = self.obs_conn.password
        if not access_key_id:
            raise Exception(f"No access_key_id is specified for connection: {self.huaweicloud_conn_id}.")

        if not access_key_secret:
            raise Exception(f"No access_key_secret is specified for connection: {self.huaweicloud_conn_id}.")

        return access_key_id, access_key_secret

    def get_default_region(self) -> str | None:
        """
        Gets region from the extra_config option in connection.
        """
        extra_config = self.obs_conn.extra_dejson
        default_region = extra_config.get("region", None)
        if not default_region:
            raise Exception(f"No region is specified for connection: {self.huaweicloud_conn_id}.")
        return default_region

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom UI field behaviour for Huawei Cloud Connection."""
        return {
            "hidden_fields": ["host", "port"],
            "relabeling": {
                "login": "Huawei Cloud Access Key ID",
                "password": "Huawei Cloud Secret Access Key",
                "schema": "Bucket Name"
            },
            "placeholders": {
                "login": "YOURACCESSKEYID",
                "password": "********",
                "schema": "obs-bucket-test",
                "extra": json.dumps(
                    {
                        "region": "cn-south-1"
                    },
                    indent=2,
                ),
            },
        }
