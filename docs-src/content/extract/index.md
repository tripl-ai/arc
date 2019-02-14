---
title: Extract
weight: 20
type: blog
---

`*Extract` stages read in data from a database or file system.

`*Extract` stages should meet this criteria:

- Read data from local or remote filesystems and return a `DataFrame`.
- Do not [transform/mutate](../transform) the data.
- Allow for [Predicate Pushdown](http://www.dbms2.com/2014/07/15/the-point-of-predicate-pushdown/) depending on data source.

File based `*Extract` stages can accept `glob` patterns as input filenames:

| Pattern | Description |
|---------|-------------|
|`*`|Matches zero or more characters.|
|`?`|Matches any single character.|
|`[abc]`|Matches a single character in the set `{a, b, c}`.|
|`[a-b]`|Matches a single character from the character range `{a...b}`.|
|`[^a-b]`|Matches a single character that is not from character set or range `{a...b}`.|
|`{a,b}`|Matches either expression `a` or `b`.|
|`\c`|Removes (escapes) any special meaning of character `c`.|
|`{ab,c{de, fg}}`|Matches a string from the string set `{ab, cde, cfg}`.|

Spark will automatically match file extensions of `.zip`, `.bz2`, `.deflate` and `.gz` and perform decompression automatically.

## AvroExtract
##### Since: 1.0.0 - Supports Streaming: False

The `AvroExtract` stage reads one or more [Apache Avro](https://avro.apache.org/) files and returns a `DataFrame`. 

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI/Glob of the input Avro files.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|URI|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/AvroExtractMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/AvroExtractComplete" highlight="json" >}} 


## BytesExtract
##### Since: 1.0.9 - Supports Streaming: False

The `BytesExtract` stage reads one or more binary files and returns a `DataFrame` containing a `Array[Byte]` of the file content (named `value`) and the file path (named `_filename`). 

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true*|Name of the incoming Spark dataset containing a list of URI/Globs to extract from.  If not present `inputURI` is requred.|
|inputURI|URI|true*|URI/Glob of the input binaryfiles. If not present `inputView` is requred.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/BytesExtractMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/BytesExtractComplete" highlight="json" >}} 


## DelimitedExtract
##### Since: 1.0.0 - Supports Streaming: True

The `DelimitedExtract` stage reads either one or more delimited text files or an input `Dataset[String]` and returns a `DataFrame`. `DelimitedExtract` will always set the underlying Spark configuration option of `inferSchema` to `false` to ensure consistent results.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true*|Name of the incoming Spark dataset. If not present `inputURI` is requred.|
|inputURI|URI|true*|URI/Glob of the input delimited text files. If not present `inputView` is requred.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|delimiter|String|false|{{< readfile file="/content/partials/fields/delimiter.md" markdown="true" >}}|
|customDelimiter|String|true*|{{< readfile file="/content/partials/fields/customDelimiter.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|header|Boolean|false|Whether or not the dataset contains a header row. If available the output dataset will have named columns otherwise columns will be named `_col1`, `_col2` ... `_colN`.<br><br>Default: `false`.|
|inputField|String|false|If using `inputView` this option allows you to specify the name of the field which contains the delimited data.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|quote|String|false|The type of quoting in the file. Supported values: `None`, `SingleQuote`, `DoubleQuote`.<br><br>Default: `DoubleQuote`.|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|URI|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/DelimitedExtractMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/DelimitedExtractComplete" highlight="json" >}} 


## HTTPExtract
##### Since: 1.0.0 - Supports Streaming: False

The `HTTPExtract` executes either a `GET` or `POST` request against a remote HTTP service and returns a `DataFrame` which will have a single row and single column holding the value of the HTTP response body. 

This stage would typically be used with a `JSONExtract` stage by specifying `inputView` instead of `inputURI` (setting `multiLine`=`true` allows processing of JSON array responses).

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true*|Name of the incoming Spark dataset containing the list of URIs in `value` field. If not present `inputURI` is requred.|
|inputURI|URI|true*|URI of the HTTP server. If not present `inputView` is requred.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|body|String|false|The request body/entity that is sent with a `POST` request.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|headers|Map[String, String]|false|{{< readfile file="/content/partials/fields/headers.md" markdown="true" >}}|
|method|String|false|The request type with valid values `GET` or `POST`.<br><br>Default: `GET`.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|validStatusCodes|Array[Integer]|false|{{< readfile file="/content/partials/fields/validStatusCodes.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/HTTPExtractMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/HTTPExtractComplete" highlight="json" >}} 


## ImageExtract
##### Since: 1.4.1 - Supports Streaming: True

The `ImageExtract` stage reads one or more image files and returns a `DataFrame` which has one column: `image`, containing image data (`jpeg`, `png`, `gif`, `bmp`, `wbmp`) stored with the schema:

| Field | Type | Description |
|-------|------|-------------|
|`origin`|String|The file path of the image.|
|`height`|Integer|The height of the image.|
|`width`|Integer|The width of the image.|
|`nChannels`|Integer|The number of image channels.|
|`mode`|Integer|OpenCV-compatible type.|
|`data`|Binary|Image bytes in OpenCV-compatible order: row-wise BGR in most cases.|

This means the image data can be accessed like:

```sql
SELECT image.height FROM dataset
```

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI/Glob of the input images.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|dropInvalid|Boolean|false|Whether to drop any invalid image files.<br><br>Default: true.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/ImageExtractMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/ImageExtractComplete" highlight="json" >}} 


## JDBCExtract
##### Since: 1.0.0 - Supports Streaming: False

The `JDBCExtract` reads directly from a JDBC Database and returns a `DataFrame`. See [Spark JDBC documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases).

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|jdbcURL|String|true|{{< readfile file="/content/partials/fields/jdbcURL.md" markdown="true" >}}|
|tableName|String|true|{{< readfile file="/content/partials/fields/tableName.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|fetchsize|Integer|false|{{< readfile file="/content/partials/fields/fetchsize.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}} This also determines the maximum number of concurrent JDBC connections.|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}}. Currently requires `user` and `password` to be set here - see example below.|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|partitionColumn|String|false|The name of a numeric column from the table in question which defines how to partition the table when reading in parallel from multiple workers. If set `numPartitions` must also be set.|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|predicates|Array[String]|false|{{< readfile file="/content/partials/fields/predicates.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|URI|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/JDBCExtractMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/JDBCExtractComplete" highlight="json" >}} 


## JSONExtract
##### Since: 1.0.0 - Supports Streaming: True

The `JSONExtract` stage reads either one or more JSON files or an input `Dataset[String]` and returns a `DataFrame`. 

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true*|Name of the incoming Spark dataset. If not present `inputURI` is requred.|
|inputURI|URI|true*|URI/Glob of the input `json` files. If not present `inputView` is requred.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|inputField|String|false|If using `inputView` this option allows you to specify the name of the field which contains the delimited data.|
|multiLine|Boolean|false|Whether the input directory contains a single JSON object per file or multiple JSON records in a single file, one per line (see [JSONLines](http://jsonlines.org/).<br><br>Default: true.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}<br><br>Additionally, by specifying the schema here, the underlying data source can skip the schema inference step, and thus speed up data loading.|
|schemaView|URI|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/JSONExtractMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/JSONExtractComplete" highlight="json" >}} 


## KafkaExtract
##### Since: 1.0.8 - Supports Streaming: True

{{< note title="Experimental" >}}
The `KafkaExtract` is currently in experimental state whilst the requirements become clearer. 

This means this API is likely to change to better handle failures.
{{</note>}}

The `KafkaExtract` stage reads records from a [Kafka](https://kafka.apache.org/) `topic` and returns a `DataFrame`. It requires a unique `groupID` to be set which on first run will consume from the `earliest` offset available in Kafka. Each subsequent run will use the offset as recorded against that `groupID`. This means that if a job fails before properly processing the data then data may need to be restarted from the earliest offset by creating a new `groupID`.

Can be used in conjuction with [KafkaCommitExecute](../execute/#kafkacommitexecute) to allow quasi-transactional behaviour (with `autoCommit` set to `false`) - in that the offset commit can be deferred until certain dependent stages are sucessfully executed.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|bootstrapServers|String|true|{{< readfile file="/content/partials/fields/bootstrapServers.md" markdown="true" >}}|
|topic|String|true|{{< readfile file="/content/partials/fields/topic.md" markdown="true" >}}|
|groupID|String|true|{{< readfile file="/content/partials/fields/groupID.md" markdown="true" >}}|
|autoCommit|Boolean|false|Whether to update the offsets in Kafka automatically. To be used in conjuction with [KafkaCommitExecute](../execute/#kafkacommitexecute) to allow quasi-transactional behaviour.<br><br>If `autoCommit` is set to `false` this stage will force `persist` equal to `true` so that Spark will not execute the Kafka extract process twice with a potentially different result (e.g. new messages added between extracts).<br><br>Default: false.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|maxPollRecords|Int|false|The maximum number of records returned in a single call to Kafka. Arc will then continue to poll until all records have been read.<br><br>Default: `10000`.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|timeout|Long|false|The time, in milliseconds, spent waiting in poll if data is not available in Kafka. Default: 10000.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/KafkaExtractMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/KafkaExtractComplete" highlight="json" >}} 


## ORCExtract
##### Since: 1.0.0 - Supports Streaming: True

The `ORCExtract` stage reads one or more [Apache ORC](https://orc.apache.org/) files and returns a `DataFrame`. 

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI/Glob of the input ORC files.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|URI|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/ORCExtractMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/ORCExtractComplete" highlight="json" >}} 


## ParquetExtract
##### Since: 1.0.0 - Supports Streaming: True

The `ParquetExtract` stage reads one or more [Apache Parquet](https://parquet.apache.org/) files and returns a `DataFrame`. 

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI/Glob of the input Parquet files.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|URI|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/ParquetExtractMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/ParquetExtractComplete" highlight="json" >}} 


## RateExtract
##### Since: 1.2.0 - Supports Streaming: True

The `RateExtract` stage creates a streaming datasource which creates rows into a streaming `DataFrame` with the signature `[timestamp: timestamp, value: long]`. 

This stage has been included for testing Structured Streaming jobs as it can be very difficult to generate test data. Generally this stage would only be included when Arc is run in a test mode (i.e. the `environment` is set to `test`).

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|rampUpTime|Integer|false|How long to ramp up before the generating speed becomes rowsPerSecond. Using finer granularities than seconds will be truncated to integer seconds.<br><br>Default: 0.|
|rowsPerSecond|Integer|false|How many rows should be generated per second.<br><br>Default: 1.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/RateExtractMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/RateExtractComplete" highlight="json" >}} 


## TextExtract
##### Since: 1.2.0 - Supports Streaming: True

The `TextExtract` stage reads either one or more text files and returns a `DataFrame`. 

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI/Glob of the input `text` files.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|multiLine|Boolean|false|Whether the to load the file as a single record or as individual records split by newline.<br><br>Default: false.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|URI|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/TextExtractMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/TextExtractComplete" highlight="json" >}} 


## XMLExtract
##### Since: 1.0.0 - Supports Streaming: False

The `XMLExtract` stage reads one or more XML files or an input `Dataset[String]` and returns a `DataFrame`. 

This extract works slightly different to the `spark-xml` package. To access the data you can use a [SQLTransform](../transform/#sqltransform) query like this which will create a new value for each row of the `bk:books` array:

```sql
SELECT EXPLODE(`bk:books`).*
FROM books_xml
```

The backtick character (`) can be used to address fields with non-alphanumeric names.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true*|URI/Glob of the input delimited  XML files. If not present `inputView` is requred.|
|inputView|String|true*|Name of the incoming Spark dataset. If not present `inputURI` is requred.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}<br><br>Additionally, by specifying the schema here, the underlying data source can skip the schema inference step, and thus speed up data loading.|
|schemaView|URI|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/XMLExtractMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/XMLExtractComplete" highlight="json" >}} 