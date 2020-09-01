---
title: Load
weight: 40
type: blog
---

`*Load` stages write out Spark `datasets` to a database or file system.

`*Load` stages should meet this criteria:

- Take in a single `dataset`.
- Perform target specific validation that the dataset has been written correctly.

## AvroLoad
##### Since: 1.0.0 - Supports Streaming: False

The `AvroLoad` writes an input `DataFrame` to a target [Apache Avro](https://avro.apache.org/) file.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputURI|URI|true|URI of the Avro file to write to.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/AvroLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/AvroLoadComplete" highlight="json" >}}


## CassandraLoad
##### Since: 2.0.0 - Supports Streaming: False
{{< note title="Plugin" >}}
The `CassandraLoad` is provided by the https://github.com/tripl-ai/arc-cassandra-pipeline-plugin package.
{{</note>}}

The `CassandraLoad` writes an input `DataFrame` to a target [Cassandra](https://cassandra.apache.org/) cluster.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|keyspace|String|true|The name of the Cassandra keyspace to write to.|
|table|String|true|The name of the Cassandra table to write to.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}} This also determines the maximum number of concurrent JDBC connections.|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}}. Any parameters provided will be added to the Cassandra connection object.|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/CassandraLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/CassandraLoadComplete" highlight="json" >}}


## ConsoleLoad
##### Since: 1.2.0 - Supports Streaming: True

The `ConsoleLoad` prints an input streaming `DataFrame` the console.

This stage has been included for testing Structured Streaming jobs as it can be very difficult to debug. Generally this stage would only be included when Arc is run in a test mode (i.e. the `environment` is set to `test`).

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|outputMode|String|false|The output mode of the console writer. Allowed values `Append`, `Complete`, `Update`. See [Output Modes](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes) for full details.<br><br>Default: `Append`|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/ConsoleLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/ConsoleLoadComplete" highlight="json" >}}


## DeltaLakeLoad
##### Since: 2.0.0 - Supports Streaming: True
{{< note title="Plugin" >}}
The `DeltaLakeLoad` is provided by the https://github.com/tripl-ai/arc-deltalake-pipeline-plugin package.
{{</note>}}

The `DeltaLakeLoad` writes an input `DataFrame` to a target [DeltaLake](https://delta.io/) file.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputURI|URI|true|URI of the Delta file to write to.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|
|generateSymlinkManifest|Boolean|false|Create a manifest file so that the DeltaLakeLoad output can be read by a Presto database.<br><br>Default: `true`|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/DeltaLakeLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/DeltaLakeLoadComplete" highlight="json" >}}


## DeltaLakeMergeLoad
##### Since: arc-deltalake-pipeline-plugin 1.7.0 - Supports Streaming: True
{{< note title="Plugin" >}}
The `DeltaLakeMergeLoad` is provided by the https://github.com/tripl-ai/arc-deltalake-pipeline-plugin package.

NOTE: This stage includes additional functionality that is not included in the main [DeltaLake](https://delta.io/) functionality. A pull request has been raised.
{{</note>}}

The `DeltaLakeMergeLoad` writes an input `DataFrame` to a target [DeltaLake](https://delta.io/) file using the `MERGE` functionality.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputURI|URI|true|URI of the Delta file to write to.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|condition|String|true|The `join condition` to perform the data comparison between the `source` (the `inputView` dataset) and `target` (the `outputURI` dataset). Note that the names `source` and `target` must be used.|
|createTableIfNotExists|Boolean|false|Create an initial `DeltaLake` table if one does not already exist.<br><br>Default: `false`|
|whenMatchedDeleteFirst|Boolean|false|If `true` the `whenMatchedDelete` operation will happen before `whenMatchedUpdate`.<br><br>If `false` the `whenMatchedUpdate` operation will happen before `whenMatchedDelete`.<br><br>Default: `true`.|
|whenMatchedDelete|Map[String, String]|false|If specified, `whenMatchedDelete` will delete records where the record exists in both `source` and `target` based on the `join condition`.<br><br>Optionally `condition` may be specified to restrict the records to delete and can only refer to fields in both `source` and `target`.|
|whenMatchedUpdate|Map[String, Object]|false|If specified, `whenMatchedUpdate` will update records where the record exists in both `source` and `target` based on the `join condition`.<br><br>Optionally `condition` may be specified to restrict the records to update and can only refer to fields in both `source` and `target`.<br><br>Optionally `values` may be specified to define the update rules which can be used to update only selected columns.|
|whenNotMatchedByTargetInsert|Map[String, Object]|false|If specified, `whenNotMatchedByTargetInsert` will insert records in `source` which do not exist in `target` based on the `join condition`.<br><br>Optionally `condition` may be specified to restrict the records to insert but can only refer to fields in `source`.<br><br>Optionally `values` may be specified to define the insert rules which can be used to insert only selected columns.|
|whenNotMatchedBySourceDelete|Map[String, Object]|false|If specified, `whenNotMatchedBySourceDelete` will delete records in `target` which do not exist in `source` based on the `join condition`.<br><br>Optionally `condition` may be specified to restrict the records to insert but can only refer to fields in `source`.|
|generateSymlinkManifest|Boolean|false|Create a manifest file so that the DeltaLakeMergeLoad output can be read by a Presto database.<br><br>Default: `true`|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/DeltaLakeMergeLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/DeltaLakeMergeLoadComplete" highlight="json" >}}



## DelimitedLoad
##### Since: 1.0.0 - Supports Streaming: True

The `DelimitedLoad` writes an input `DataFrame` to a target delimited file.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputURI|URI|true|URI of the Delimited file to write to.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|customDelimiter|String|true*|{{< readfile file="/content/partials/fields/customDelimiter.md" markdown="true" >}}|
|delimiter|String|false|{{< readfile file="/content/partials/fields/delimiter.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|header|Boolean|false|Whether to write a header row.<br><br>Default: `false`.|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|quote|String|false|The type of quoting in the file. Supported values: `None`, `SingleQuote`, `DoubleQuote`.<br><br>Default: `DoubleQuote`.|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/DelimitedLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/DelimitedLoadComplete" highlight="json" >}}


## ElasticsearchLoad
##### Since: 1.9.0 - Supports Streaming: False
{{< note title="Plugin" >}}
The `ElasticsearchLoad` is provided by the https://github.com/tripl-ai/arc-elasticsearch-pipeline-plugin package.
{{</note>}}

The `ElasticsearchLoad` writes an input `DataFrame` to a target [Elasticsearch](https://www.elastic.co/products/elasticsearch) cluster.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|output|String|true|The name of the target Elasticsearch index.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Parameters for connecting to the [Elasticsearch](https://www.elastic.co/products/elasticsearch) cluster are detailed [here](https://www.elastic.co/guide/en/elasticsearch/hadoop/master/configuration.html).|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/ElasticsearchLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/ElasticsearchLoadComplete" highlight="json" >}}


## HTTPLoad
##### Since: 1.0.0 - Supports Streaming: True

The `HTTPLoad` takes an input `DataFrame` and executes a series of `POST` requests against a remote HTTP service. The input to this stage needs to be a single column dataset of signature `value: string` and is intended to be used after a [JSONTransform](/load/#jsontransform) stage which would prepare the data for sending to the external server.

In the future additional Transform stages (like `ProtoBufTransform`) could be added to prepare binary payloads instead of just `json` `string`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputURI|URI|true|URI of the HTTP server.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|headers|Map[String, String]|false|{{< readfile file="/content/partials/fields/headers.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|validStatusCodes|Array[Integer]|false|{{< readfile file="/content/partials/fields/validStatusCodes.md" markdown="true" >}} Note: all request response codes must be contained in this list for the stage to be successful.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/HTTPLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/HTTPLoadComplete" highlight="json" >}}


## JDBCLoad
##### Since: 1.0.0 - Supports Streaming: True

The `JDBCLoad` writes an input `DataFrame` to a target JDBC Database. See [Spark JDBC documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases).

Whilst it is possible to use `JDBCLoad` to create tables directly in the target database Spark only has a limited knowledge of the schema required in the destination database and so will translate things like `StringType` internally to a `TEXT` type in the target database (because internally Spark does not have limited length strings). The recommendation is to use a preceding [JDBCExecute](../execute/#jdbcexecute) to execute a `CREATE TABLE` statement which creates the intended schema then inserting into that table with `saveMode` set to `Append`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|jdbcURL|String|true|{{< readfile file="/content/partials/fields/jdbcURL.md" markdown="true" >}}|
|tableName|String|true|The target JDBC table. Must be in `database`.`schema`.`table` format.|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}}. Any parameters provided will be added to the JDBC connection object. These are not logged so it is safe to put passwords here.|
|batchsize|Integer|false|{{< readfile file="/content/partials/fields/batchsize.md" markdown="true" >}}|
|bulkload|Boolean|false|{{< readfile file="/content/partials/fields/bulkload.md" markdown="true" >}}|
|createTableColumnTypes|String|false|{{< readfile file="/content/partials/fields/createTableColumnTypes.md" markdown="true" >}}|
|createTableOptions|String|false|{{< readfile file="/content/partials/fields/createTableOptions.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|isolationLevel|String|false|{{< readfile file="/content/partials/fields/isolationLevel.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}} This also determines the maximum number of concurrent JDBC connections.|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|
|tablock|Boolean|false|When in `bulkload` mode whether to set `TABLOCK` on the driver.<br><br>Default: `true`.|
|truncate|Boolean|false|{{< readfile file="/content/partials/fields/truncate.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/JDBCLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/JDBCLoadComplete" highlight="json" >}}


## JSONLoad
##### Since: 1.0.0 - Supports Streaming: True

The `JSONLoad` writes an input `DataFrame` to a target JSON file.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputURI|URI|true|URI of the Delimited file to write to.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/JSONLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/JSONLoadComplete" highlight="json" >}}


## KafkaLoad
##### Since: 1.0.8 - Supports Streaming: True
{{< note title="Plugin" >}}
The `KafkaLoad` is provided by the https://github.com/tripl-ai/arc-kafka-pipeline-plugin package.
{{</note>}}

The `KafkaLoad` writes an input `DataFrame` to a target [Kafka](https://kafka.apache.org/) `topic`. The input to this stage needs to be a single column dataset of signature `value: string` - intended to be used after a [JSONTransform](/load/#jsontransform) stage - or a two columns of signature `key: string, value: string` which could be created by a [SQLTransform](/load/#sqltransform) stage.

In the future additional Transform stages (like `ProtoBufTransform`) may be added to prepare binary payloads instead of just `json` `string`.


### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|bootstrapServers|String|true|{{< readfile file="/content/partials/fields/bootstrapServers.md" markdown="true" >}}|
|topic|String|true|{{< readfile file="/content/partials/fields/topic.md" markdown="true" >}}|
|acks|Integer|false|{{< readfile file="/content/partials/fields/acks.md" markdown="true" >}}<br><br>Default: `1`.|
|batchSize|Integer|false|Number of records to send in single requet to reduce number of requests to Kafka.<br><br>Default: `16384`.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|retries|Integer|false|How many times to try to resend any record whose send fails with a potentially transient error.<br><br>Default: `0`.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/KafkaLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/KafkaLoadComplete" highlight="json" >}}


## MongoDBLoad
##### Since: 2.0.0 - Supports Streaming: False
{{< note title="Plugin" >}}
The `MongoDBLoad` is provided by the https://github.com/tripl-ai/arc-mongo-pipeline-plugin package.
{{</note>}}

The `MongoDBLoad` writes an input `DataFrame` to a target [MongoDB](https://www.mongodb.com/) collection.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|options|Map[String, String]|false|Map of configuration parameters. These parameters are used to provide database connection/collection details.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/MongoDBLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/MongoDBLoadComplete" highlight="json" >}}


## ORCLoad
##### Since: 1.0.0 - Supports Streaming: True

The `ORCLoad` writes an input `DataFrame` to a target [Apache ORC](https://orc.apache.org/) file.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputURI|URI|true|URI of the ORC file to write to.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/ORCLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/ORCLoadComplete" highlight="json" >}}


## ParquetLoad
##### Since: 1.0.0 - Supports Streaming: True

The `ParquetLoad` writes an input `DataFrame` to a target [Apache Parquet](https://parquet.apache.org/) file.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputURI|URI|true|URI of the Parquet file to write to.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/ParquetLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/ParquetLoadComplete" highlight="json" >}}


## TextLoad
##### Since: 1.9.0 - Supports Streaming: False

The `TextLoad` writes an input `DataFrame` to a target text file.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputURI|URI|true|URI of the Parquet file to write to.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|
|singleFile|Boolean|false|Write in single file mode instead of a directory containing one or more partitions. Accepts datasets with either `[value: string]`, `[value: string, filename: string]` or `[value: string, filename: string, index: integer]` schema. If `filename` is supplied this component will write to one or more files. If `index` is supplied this component will order the records in each `filename` by `index` before writing.|
|prefix|String|false|A string to append before the row data when in `singleFile` mode.|
|separator|String|false|A separator string to append between the row data when in `singleFile` mode. Most common use will be `\n` which will insert newlines.|
|suffix|String|false|A string to append after the row data when in `singleFile` mode.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/TextLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/TextLoadComplete" highlight="json" >}}


## XMLLoad
##### Since: 1.0.0 - Supports Streaming: False

The `XMLLoad` writes an input `DataFrame` to a target XML file.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputURI|URI|true|URI of the XML file to write to.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|
|singleFile|Boolean|false|Write in single file mode instead of a directory containing one or more partitions. Accepts datasets with either `[value: string]`, `[value: string, filename: string]` or `[value: string, filename: string, index: integer]` schema. If `filename` is supplied this component will write to one or more files. If `index` is supplied this component will order the records in each `filename` by `index` before writing.|
|prefix|String|false|A string to append before the row data when in `singleFile` mode. Useful for specifying the encoding `<?xml version="1.0" encoding="UTF-8"?>`.|

### Examples

The XML writer uses reserved keywords to be able to set attributes as well as values. The `_VALUE` value is reserved to define values when attributes which are prefixed with an underscore (`_`) exist:

```sql
SELECT
  NAMED_STRUCT(
    '_VALUE', NAMED_STRUCT(
      'child0', 0,
      'child1', NAMED_STRUCT(
        'nested0', 0,
        'nested1', 'nestedvalue'
      )
    ),
    '_attributeName', 'attributeValue'
  ) AS Document
```

Results in:

```xml
<Document attributeName="attributeValue">
  <child0>1</child0>
  <child1>
    <nested0>1</nested0>
    <nested1>nestedvalue</nested1>
  </child1>
</Document>
```

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/XMLLoadMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/XMLLoadComplete" highlight="json" >}}
