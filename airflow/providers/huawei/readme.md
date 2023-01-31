# Airflow对接OBS插件接口文档

## huawei.cloud.operators.huawei_obs

| 类名                                                                | 功能         |
|-------------------------------------------------------------------|------------|
| [OBSCreateBucketOperator](#obscreatebucketoperator)               | 创建OBS桶     |
| [OBSListBucketOperator](#obslistbucketoperator)                   | 列举OBS桶     |
| [OBSDeleteBucketOperator](#obsdeletebucketoperator)               | 删除OBS桶     |
| [OBSListObjectsOperator](#obslistobjectsoperator)                 | 列举OBS桶中对象  |
| [OBSGetBucketTaggingOperator](#obsgetbuckettaggingoperator)       | 获取OBS桶标签   |
| [OBSSetBucketTaggingOperator](#obssetbuckettaggingoperator)       | 设置OBS桶标签   |
| [OBSDeleteBucketTaggingOperator](#obsdeletebuckettaggingoperator) | 删除OBS桶标签   |
| [OBSCreateObjectOperator](#obscreateobjectoperator)               | 创建OBS桶对象   |
| [OBSGetObjectOperator](#obsgetobjectoperator)                     | 从OBS桶获取对象  |
| [OBSCopyObjectOperator](#obscopyobjectoperator)                   | 复制OBS桶对象   |
| [OBSDeleteObjectOperator](#obsdeleteobjectoperator)               | 删除OBS桶对象   |
| [OBSDeleteBatchObjectOperator](#obsdeletebatchobjectoperator)     | 批量删除OBS桶对象 |
| [OBSMoveObjectOperator](#obsmoveobjectoperator)                   | 移动OBS桶对象   |

____

### Airflow Connection配置

Airflow连接信息配置： WebUI -> Admin -> Connections

**参数描述：**
> Connection Id: 连接id，填入huaweicloud_conn_id
> Connection Type: 连接类型，选择OBS
> OBS Bucket Name: 默认OBS桶名
> Huawei Cloud Access Key ID：AK
> Huawei Cloud Secret Access Key：SK
> Extra: {"region": "{yourself-region}"}

____

### OBSCreateBucketOperator

***创建OBS桶***
|参数|类型|约束|描述|
| -- | -- | -- | -- |
|bucket_name|str|必选|将被创建的OBS桶名|
|huaweicloud_conn_id|str|可选|用于OBS凭据的Airflow连接，需在Airflow中配置。默认使用huaweicloud_default连接（需提前创建）|
|region|str|可选|OBS区域，默认从huaweicloud_conn_id连接中选项Extra下region参数获取|

**示例：**

```python
create_bucket = OBSCreateBucketOperator(
  task_id='create_bucket',
  bucket_name = 'obs-yourself-bucket',
  huaweicloud_conn_id = 'obs-yourself-conn',
)
```

____

### OBSListBucketOperator

***列举OBS桶***
|参数|类型|约束|描述|
| -- | -- | -- | -- |
|huaweicloud_conn_id|str|可选|用于OBS凭据的Airflow连接，需在Airflow中配置。默认使用huaweicloud_default连接（需提前创建）|
|region|str|可选|OBS区域，默认从huaweicloud_conn_id连接中选项Extra下region参数获取</br> 设为"cn-north-1"时可列举所有区域的OBS桶|

**示例：**

```python
list_bucket = OBSListBucketOperator(
  task_id='list_bucket',
  huaweicloud_conn_id = 'obs-yourself-conn',
)
```

**返回结果:**

```python
[
  {"name": "mock_bucket1",
   "region": "cn-south-1",
   "create_date": "2023/01/13 10:00:00",
   "bucket_type": "OBJECT",},
  {"name": "mock_bucket2",
   "region": "cn-south-1",
   "create_date": "2023/01/14 10:00:00",
   "bucket_type": "OBJECT",
   }
 ]
```

____

### OBSDeleteBucketOperator

***删除OBS桶***
|参数|类型|约束|描述|
| -- | -- | -- | -- |
|huaweicloud_conn_id|str|可选|用于OBS凭据的Airflow连接，需在Airflow中配置。默认使用huaweicloud_default连接（需提前创建）|
|region|str|可选|OBS区域，默认从huaweicloud_conn_id连接中选项Extra下region参数获取</br> 设为"cn-north-1"时可删除任意区域的OBS桶|
|bucket_name|str|可选|将被删除的OBS桶名|

> 无法直接删除非空桶

**示例：**

```python
delete_bucket = OBSDeleteBucketOperator(
  task_id='delete_bucket',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-yourself-bucket',
)
```

____

### OBSListObjectsOperator

***列举OBS桶中对象***
|参数|类型|约束|描述|
| -- | -- | -- | -- |
|huaweicloud_conn_id|str|可选|用于OBS凭据的Airflow连接，需在Airflow中配置。默认使用huaweicloud_default连接（需提前创建）|
|region|str|可选|OBS区域，默认从huaweicloud_conn_id连接中选项Extra下region参数获取</br> 设为"cn-north-1"时可列举任意区域的OBS桶对象|
|bucket_name|str|可选|被列举对象的OBS桶名，默认从huaweicloud_conn_id连接中的shema选项获取|
|prefix|str|可选|限定返回的对象名必须带有prefix前缀。|
|marker|str|可选|列举桶内对象列表时，指定一个标识符，从该标识符以后按字典顺序返回对象列表。|
|max_keys|int|可选|列举对象的最大数目，取值范围为1~1000，当超出范围时，按照默认的1000进行处理。|
|is_truncated|bool|可选|是否开启分页查询，列举符合条件的所有对象|

**示例：**

**简单列举**
以下代码展示如何简单列举对象，最多返回1000个对象：

```python
list_object = OBSListObjectsOperator(
  task_id='list_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
)
```

**指定数目列举**
以下代码展示如何指定数目列举对象：

```python
list_object = OBSListObjectsOperator(
  task_id='list_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  # 只列举100个对象
  max_keys = '100',
)
```

**指定前缀列举**
以下代码展示如何指定前缀列举对象：

```python
list_object = OBSListObjectsOperator(
  task_id='list_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  # 列举100个带prefix前缀的对象
  prefix = 'prefix',
  max_keys = '100',
)
```

**指定起始位置列举**
以下代码展示如何指定起始位置列举对象：

```python
list_object = OBSListObjectsOperator(
  task_id='list_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  # 列举对象名字典序在"test"之后的100个对象
  marker = 'test',
  max_keys = '100',
)
```

**分页列举全部对象**
以下代码展示分页列举全部对象:

```python
list_object = OBSListObjectsOperator(
  task_id='list_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  # 开启分页查询，设置每页100个对象
  max_keys = '100',
  is_truncated = True,
)
```

**列举文件夹中的所有对象**
OBS本身是没有文件夹的概念的，桶中存储的元素只有对象。文件夹对象实际上是一个大小为0且对象名以“/”结尾的对象，将这个文件夹对象名作为前缀，即可模拟列举文件夹中对象的功能。以下代码展示如何列举文件夹中的对象：

```python
list_object = OBSListObjectsOperator(
  task_id='list_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  # 开启分页查询，设置文件夹对象名"dir/"为前缀
  prefix = 'dir/',
  max_keys = '100',
  is_truncated = True,
)
```

**返回结果**：

```python
# 未开启分页查询
['key1', 'key2']

# 开启分页查询 is_truncated = True
[['key1', 'key2', ...], ['key3', 'key4', ...], ...]
```

____

### OBSGetBucketTaggingOperator

***获取OBS桶标签***
|参数|类型|约束|描述|
| -- | -- | -- | -- |
|huaweicloud_conn_id|str|可选|用于OBS凭据的Airflow连接，需在Airflow中配置。默认使用huaweicloud_default连接（需提前创建）|
|region|str|可选|OBS区域，默认从huaweicloud_conn_id连接中选项Extra下region参数获取</br> 设为"cn-north-1"时可获取任意区域的OBS桶标签|
|bucket_name|str|可选|将被获取桶标签的OBS桶名，默认从huaweicloud_conn_id连接中的shema选项获取|

**示例：**

```python
get_bucket_tagging = OBSGetBucketTaggingOperator(
  task_id='get_bucket_tagging',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
)
```

返回结果：

```python
[{'key': 'app1', 'value': 'test1'}, {'key': 'app2', 'value': 'test2'}]
```

____

### OBSSetBucketTaggingOperator

***设置OBS桶标签***
|参数|类型|约束|描述|
| -- | -- | -- | -- |
|tag_info|dict|必选|桶标签配置|
|bucket_name|str|可选|将被设置桶标签的OBS桶名，默认从huaweicloud_conn_id连接中的shema选项获取|
|huaweicloud_conn_id|str|可选|用于OBS凭据的Airflow连接，需在Airflow中配置。默认使用huaweicloud_default连接（需提前创建）|
|region|str|可选|OBS区域，默认从huaweicloud_conn_id连接中选项Extra下region参数获取</br> 设为"cn-north-1"时可设置任意区域的OBS桶标签|

**示例：**

```python
set_bucket_tagging = OBSSetBucketTaggingOperator(
  task_id='list_bucket',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  tag_ingo = {
    "key1": 'value1',
    "key2": 'value2',
  }
)
```

____

### OBSDeleteBucketTaggingOperator

***删除OBS桶标签***
|参数|类型|约束|描述|
| -- | -- | -- | -- |
|huaweicloud_conn_id|str|可选|用于OBS凭据的Airflow连接，需在Airflow中配置。默认使用huaweicloud_default连接（需提前创建）|
|region|str|可选|OBS区域，默认从huaweicloud_conn_id连接中选项Extra下region参数获取</br> 设为"cn-north-1"时可删除任意区域的OBS桶标签|
|bucket_name|str|可选|将被删除桶标签的OBS桶名，默认从huaweicloud_conn_id连接中的shema选项获取|

**示例：**

```python
delete_bucket_tagging = OBSDeleteBucketTaggingOperator(
  task_id='delete_bucket_tagging',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
)
```

____

### OBSCreateObjectOperator

***上传文件对象到OBS桶***
|参数|类型|约束|描述|
| -- | -- | -- | -- |
|object_key|str|必选|对象的OBS路径, 如配置参数Connections中提供桶名，应为相对路径|
|data|str\|object|必选|待上传数据。 </br>使用字符串作为对象的数据源，上传文本到指定桶。 </br>流式上传使用包含“read”属性的可读对象作为对象的数据源，以网络流或文件流方式上传数据到指定桶。 </br>待上传文件/文件夹的完整路径，如/aa/bb.txt，或/aa/。 </br>如果data是文件夹，则md5会被忽略。|
|object_type|str|可选|上传对象类型，默认为content，类型如下 </br>file: data参数为待上传文件/文件夹的完整路径，如/aa/bb.txt，或/aa/。 </br>content: data参数为使用字符串作为对象的数据源，上传文本到指定桶，或者使用包含“read”属性的可读对象作为对象的数据源，以网络流或文件流方式上传数据到指定桶。|
|huaweicloud_conn_id|str|可选|用于OBS凭据的Airflow连接，需在Airflow中配置。默认使用huaweicloud_default连接（需提前创建）|
|region|str|可选|OBS区域，默认从huaweicloud_conn_id连接中选项Extra下region参数获取|
|bucket_name|str|可选|将被删除桶标签的OBS桶名，默认从huaweicloud_conn_id连接中的shema选项获取|
|metadata|dict|可选|上传文件的自定义元数据。|
|md5|str|可选|待上传对象数据的MD5值（经过Base64编码），提供给OBS服务端，校验数据完整性。|
|acl|str|可选|上传对象时可指定的预定义访问策略，访问策略如下：</br> PRIVATE，私有读写</br> PUBLIC_READ，公共读</br> PUBLIC_READ_WRITE，公共读写</br> BUCKET_OWNER_FULL_CONTROL，桶或对象所有者拥有完全控制权限。|
|storage_class|str|可选|上传对象时可指定的对象的存储类型，存储类型如下:</br> STANDARD，标准存储，拥有低访问时延和较高的吞吐量，适用于有大量热点对象（平均一个月多次）或小对象（<1MB），且需要频繁访问数据的业务场景。</br> WARM，低频访问存储，适用于不频繁访问（平均一年少于12次）但在需要时也要求能够快速访问数据的业务场景。</br> COLD，归档存储，适用于很少访问（平均一年访问一次）数据的业务场景。|
|expires|int|可选|待上传对象的生命周期，单位：天|

**示例：**

**文件上传**
将本地文件 /tmp/readme.md 上传为OBS桶对象，对象名为 test/upload/readme.md

```python
create_object = OBSCreateObjectOperator(
  task_id='create_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  object_key = 'test/upload/readme.md',
  object_type = 'file',
  data = '/tmp/readme.md'
)
```

**文件加密上传**
将本地文件 /tmp/readme.md 上传为OBS桶对象，对象名为 test/upload/readme.md

```python
create_object = OBSCreateObjectOperator(
  task_id='create_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  object_key = 'test/upload/readme.md',
  object_type = 'file',
  data = '/tmp/readme.md',
  # 数据加密服务 DEW中密钥管理下对应秘钥ID，默认秘钥为obs/default的ID
  encryption = 'kms',
  key = 'DEW-yourself-secret',
)
```

**文件夹上传**
将本地文件夹 /tmp/demo/ 上传为OBS桶对象，对象名为 test/upload/demo,
如果有文件上传失败，返回失败对象列表

```python
create_object = OBSCreateObjectOperator(
  task_id='create_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  object_key = 'test/upload/demo',
  object_type = 'file',
  data = '/tmp/demo'
)
```

**上传文本**
使用字符串作为对象的数据源，上传文本到指定桶，对象名为 test/text.txt,

```python
data = 'upload a string'
create_object = OBSCreateObjectOperator(
  task_id='create_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  object_key = 'test/text.txt',
  object_type = 'content',
  data = data
)
```

**上传文件流**
将文件流 /tmp/stream.txt 上传为OBS桶对象，对象名为 /tmp/stream.txt,

```python
data = open('/tmp/readme.md', 'rb')
create_object = OBSCreateObjectOperator(
  task_id='create_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  object_key = '/tmp/stream.txt',
  data = data
)
```

**上传网络流**
将网络流上传为OBS桶对象，对象名为 test/netstream.html,

```python
import http.client as httplib
conn = httplib.HTTPConnection('www.huaweicloud.com', 80)
conn.request('GET', '/')
data = conn.getresponse()
create_object = OBSCreateObjectOperator(
  task_id='create_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  object_key = 'test/netstream.html',
  object_type = 'content',
  data = data
)
```

____

### OBSGetObjectOperator

***从OBS桶下载对象***
|参数|类型|约束|描述|
| -- | -- | -- | -- |
|object_key|str|必选|key of the object to get, 如配置参数Connections中提供桶名，应为相对路径|
|download_path|str|可选|下载对象的目标路径，包含文件名，如aa/bb.txt.|
|load_stream_in_memory|str|可选|是否将对象的数据流加载到内存。 </br>默认值为False，如果该值为True，会忽略download_path参数，并返回将获取的数据流。|
|huaweicloud_conn_id|str|可选|用于OBS凭据的Airflow连接，需在Airflow中配置。默认使用huaweicloud_default连接（需提前创建）|
|region|str|可选|OBS区域，默认从huaweicloud_conn_id连接中选项Extra下region参数获取</br> 设为"cn-north-1"时可从任意区域的OBS桶下载对象|
|bucket_name|str|可选|下载对象的OBS桶名，默认从huaweicloud_conn_id连接中的shema选项获取|

**示例：**

**文件下载**
以文件形式下载指定桶中的对象

```python
download_object = OBSGetObjectOperator(
  task_id='download_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  object_key = 'test/upload/readme.md',
  download_path = 'tmp/download/readme.md',
)
```

**数据流下载**
返回指定桶中的对象的数据流

```python
download_object = OBSGetObjectOperator(
  task_id='download_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  object_key = 'test/upload/readme.md',
  load_stream_in_memory = True,
)

```

____

### OBSCopyObjectOperator

***复制OBS桶对象***
|参数|类型|约束|描述|
| -- | -- | -- | -- |
|source_object_key|str|必选|源对象名|
|dest_object_key|str|必选|目标对象名|
|huaweicloud_conn_id|str|可选|用于OBS凭据的Airflow连接，需在Airflow中配置。默认使用huaweicloud_default连接（需提前创建）|
|region|str|可选|OBS区域，默认从huaweicloud_conn_id连接中选项Extra下region参数获取</br> 设为"cn-north-1"时可复制任意区域的OBS桶对象|
|source_bucket_name|str|可选|源OBS桶名|
|dest_bucket_name|str|可选|目标OBS桶名|
|versionId|str|可选|源对象版本号，非多版本桶不考虑|
|metadata|dict|可选|目标对象的自定义元数据(需要指定directive为'REPLACE')|
|directive|str|可选|目标对象的属性是否从源对象中复制，支持的值：</br> COPY: 默认值，从源对象复制</br> REPLACE: 以请求参数中指定的值替换|
|acl|str|可选|复制对象时可指定的预定义访问策略。策略如下：</br> PRIVATE，私有读写。</br> PUBLIC_READ 公共读。</br> PUBLIC_READ_WRITE，公共读写。</br> BUCKET_OWNER_FULL_CONTROL，桶或对象所有者拥有完全控制权限。|

> 不支持跨区域复制

**示例：**

```python
copy_object = OBSCopyObjectOperator(
  task_id='copy_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  source_object_key = 'obs-youself-bucket',
  dest_object_key = 'obs-youself-bucket',
  source_bucket_name = 'obs-youself-bucket',
  dest_bucket_name = 'obs-youself-bucket',
)
```

____

### OBSDeleteObjectOperator

***删除OBS桶对象***
|参数|类型|约束|描述|
| -- | -- | -- | -- |
|object_key|str|必选|将被删除的对象名, 如配置参数Connections中提供桶名，应为相对路径|
|huaweicloud_conn_id|str|可选|用于OBS凭据的Airflow连接，需在Airflow中配置。默认使用huaweicloud_default连接（需提前创建）|
|region|str|可选|OBS区域，默认从huaweicloud_conn_id连接中选项Extra下region参数获取</br> 设为"cn-north-1"时可删除任意区域的OBS桶对象|
|bucket_name|str|可选|将被删除对象的OBS桶名，默认从huaweicloud_conn_id连接中的shema选项获取|
|version_id|str|可选|对象版本号，非多版本桶不考虑|

**示例：**

```python
delete_object = OBSDeleteObjectOperator(
  task_id='delete_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  object_key = 'test/upload/readme.md',
)
```

____

### OBSDeleteBatchObjectOperator

***批量删除OBS桶对象***
|参数|类型|约束|描述|
| -- | -- | -- | -- |
|object_list|list|必选|待删除的对象列表</br> 对象非多版本表示方式 ['object_key1', 'object_key2', ...]</br> 对象存在多版本表示方式[{'object_key': 'test_key', 'version_id': 'test_version'}, ...] or ['object_key1', 'object_key2', ...]|
|huaweicloud_conn_id|str|可选|用于OBS凭据的Airflow连接，需在Airflow中配置。默认使用huaweicloud_default连接（需提前创建）|
|region|str|可选|OBS区域，默认从huaweicloud_conn_id连接中选项Extra下region参数获取</br> 设为"cn-north-1"时可删除任意区域的OBS桶对象|
|bucket_name|str|可选|将被删除桶标签的OBS桶名，默认从huaweicloud_conn_id连接中的shema选项获取|
|quiet|str|可选|批量删除对象的响应方式</br> False表示详细模式，返回的删除成功和删除失败的所有结果</br> True表示简单模式，只返回的删除过程中出错的结果。|

**示例：**

```python
delete_objects = OBSDeleteBatchObjectOperator(
  task_id='delete_objects',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  object_list = ['test/a.txt', 'test/b.txt'],
)
```

____

### OBSMoveObjectOperator

***移动OBS桶对象***
|参数|类型|约束|描述|
| -- | -- | -- | -- |
|source_object_key|str|必选|源对象名|
|dest_object_key|str|必选|目标对象名|
|huaweicloud_conn_id|str|可选|用于OBS凭据的Airflow连接，需在Airflow中配置。默认使用huaweicloud_default连接（需提前创建）|
|region|str|可选|OBS区域，默认从huaweicloud_conn_id连接中选项Extra下region参数获取</br> 设为"cn-north-1"时可移动任意区域的OBS桶对象|
|bucket_name|str|可选|将进行移动对象操作的OBS桶名，默认从huaweicloud_conn_id连接中的shema选项获取|

> 不支持跨区域移动

**示例：**

```python
move_object = OBSMoveObjectOperator(
  task_id='move_object',
  huaweicloud_conn_id = 'obs-yourself-conn',
  bucket_name = 'obs-youself-bucket',
  source_object_key = 'test/upload/readme.md',
  dest_object_key = 'test/move/readme.md',
)
```

____
