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
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/AvroLoadMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/AvroLoadComplete" highlight="json" >}} 

## AzureEventHubsLoad
##### Since: 1.0.0 - Supports Streaming: False

The `AzureEventHubsLoad` writes an input `DataFrame` to a target [Azure Event Hubs](https://azure.microsoft.com/en-gb/services/event-hubs/) stream. The input to this stage needs to be a single column dataset of signature `value: string` and is intended to be used after a [JSONTransform](/load/#jsontransform) stage which would prepare the data for sending to the external server.

In the future additional Transform stages (like `ProtoBufTransform`) could be added to prepare binary payloads instead of just `json` `string`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|namespaceName|String|true|{{< readfile file="/content/partials/fields/namespaceName.md" markdown="true" >}}|
|eventHubName|String|true|{{< readfile file="/content/partials/fields/eventHubName.md" markdown="true" >}}|
|sharedAccessSignatureKeyName|String|true|{{< readfile file="/content/partials/fields/sharedAccessSignatureKeyName.md" markdown="true" >}}|
|sharedAccessSignatureKey|String|true|{{< readfile file="/content/partials/fields/sharedAccessSignatureKey.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}} Azure EventHubs will throw a `ServerBusyException` if too many executors write to a target in parallel which can be decreased by reducing the number of partitions.|
|retryCount|Integer|false|The maximum number of retries for the exponential backoff algorithm.<br><br>Default: 10.|
|retryMaxBackoff|Long|false|The maximum time (in seconds) for the exponential backoff algorithm to wait between retries.<br><br>Default: 30.|
|retryMinBackoff|Long|false|The minimum time (in seconds) for the exponential backoff algorithm to wait between retries.<br><br>Default: 0.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/AzureEventHubsLoadMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/AzureEventHubsLoadComplete" highlight="json" >}} 


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
|outputMode|String|false|The output mode of the console writer. Allowed values `Append`, `Complete`, `Update`. See [Output Modes](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes) for full details.<br><br>Default: `Append`|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/ConsoleLoadMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/ConsoleLoadComplete" highlight="json" >}} 


## DatabricksDeltaLoad
##### Since: 1.8.0 - Supports Streaming: True

{{< note title="Experimental" >}}
The `DatabricksDeltaLoad` is currently in experimental state whilst the requirements become clearer. 

This means this API is likely to change.
{{</note>}}

The `DatabricksDeltaLoad` writes an input `DataFrame` to a target [Databricks Delta](https://databricks.com/product/databricks-delta/) file. 

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputURI|URI|true|URI of the Delta file to write to.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/DatabricksDeltaLoadMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/DatabricksDeltaLoadComplete" highlight="json" >}} 


## DatabricksSQLDWLoad
##### Since: 1.8.1 - Supports Streaming: False

{{< note title="Experimental" >}}
The `DatabricksSQLDWLoad` is currently in experimental state whilst the requirements become clearer. 

This means this API is likely to change.
{{</note>}}

The `DatabricksSQLDWLoad` writes an input `DataFrame` to a target [Azure SQL Data Warehouse](https://azure.microsoft.com/en-au/services/sql-data-warehouse/) file using a [proprietary driver](https://docs.databricks.com/spark/latest/data-sources/azure/sql-data-warehouse.html) within a Databricks Runtime Environment.

Known limitations:

- SQL Server date fields can only be between range `1753-01-01` to `9999-12-31`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|jdbcURL|URI|true|URI of the Delta file to write to.|
|dbTable|String|true|The table to create in SQL DW.|
|tempDir|URI|true|A Azure Blob Storage path to temporarily hold the data before executing the SQLDW load.|
|authentication|Map[String, String]|true|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}. Note this stage only works with the `AzureSharedKey` [authentication](../partials/#authentication) method.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|forwardSparkAzureStorageCredentials|Boolean|false|If true, the library automatically discovers the credentials that Spark is using to connect to the Blob Storage container and forwards those credentials to SQL DW over JDBC.<br><br>Default: `true`.|
|tableOptions|String|false|Used to specify table options when creating the SQL DW table.|
|maxStrLength|Integer|false|The default length of `String`/`NVARCHAR` columns when creating the table in SQLDW.<br><br>Default: `256`.|
|params|Map[String, String]|false|Parameters for connecting to the [Azure SQL Data Warehouse](https://azure.microsoft.com/en-au/services/sql-data-warehouse/) so that password is not logged.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/DatabricksSQLDWLoadMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/DatabricksSQLDWLoadComplete" highlight="json" >}} 


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

{{< note title="Experimental" >}}
The `ElasticsearchLoad` is currently in experimental state whilst the requirements become clearer. 

This means this API is likely to change.
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
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Parameters for connecting to the [Elasticsearch](https://www.elastic.co/products/elasticsearch) cluster are detailed [here](https://www.elastic.co/guide/en/elasticsearch/hadoop/master/configuration.html).|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/ElasticsearchLoadMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/ElasticsearchLoadComplete" highlight="json" >}} 


## HTTPLoad
##### Since: 1.0.0 - Supports Streaming: False

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
|params|Map[String, String]|true|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}}. Currently requires `user` and `password` to be set here - see example below.|
|batchsize|Integer|false|{{< readfile file="/content/partials/fields/batchsize.md" markdown="true" >}}|
|bulkload|Boolean|false|{{< readfile file="/content/partials/fields/bulkload.md" markdown="true" >}}|
|createTableColumnTypes|String|false|{{< readfile file="/content/partials/fields/createTableColumnTypes.md" markdown="true" >}}|
|createTableOptions|String|false|{{< readfile file="/content/partials/fields/createTableOptions.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
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
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|retries|Integer|false|How many times to try to resend any record whose send fails with a potentially transient error.<br><br>Default: `0`.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/KafkaLoadMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/KafkaLoadComplete" highlight="json" >}} 


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
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|
|singleFile|Boolean|false|Write to a single text file instead of a directory containing one or more partitions. Warning: this will pull the entire dataset to memory on the driver process so will not work for large datasets unless the driver has a sufficiently large memory allocation.|
|prefix|String|false|A string to append before the row data when in `singleFile` mode.|
|separator|String|false|A separator string to append between the row data when in `singleFile` mode.|
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
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|saveMode|String|false|{{< readfile file="/content/partials/fields/saveMode.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/XMLLoadMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/XMLLoadComplete" highlight="json" >}} 
