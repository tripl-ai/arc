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

File based `*Extract` stages can accept `glob` patterns as input filenames which can be very useful to load just a subset of data. For example [delta processing](../solutions/#delta-processing):

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

Spark will automatically match file extensions of `.bz2`, `.deflate` and `.gz` and perform decompression automatically.

## AvroExtract
##### Since: 1.0.0 - Supports Streaming: False

The `AvroExtract` stage reads one or more [Apache Avro](https://avro.apache.org/) files and returns a `DataFrame`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true*|URI/Glob of the input delimited  Avro files. If not present `inputView` is requred.|
|inputView|String|true*|Name of the incoming Spark dataset. If not present `inputURI` is requred.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|basePath|URI|false|{{< readfile file="/content/partials/fields/basePath.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|String|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|
|inputField|String|false|If using `inputView` this option allows you to specify the name of the field which contains the Avro binary data.|
|avroSchemaView|URI|false*|If using `inputView` this option allows you to specify the Avro schema URI. Has been tested to work with the [Kafka Schema Registry](https://www.confluent.io/confluent-schema-registry/) with URI like `http://kafka-schema-registry:8081/schemas/ids/1` as well as standalone `*.avsc` files.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/AvroExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/AvroExtractComplete" highlight="json" >}}


## BigQueryExtract
##### Supports Streaming: False
{{< note title="Plugin" >}}
The `BigQueryExtract` is provided by the https://github.com/tripl-ai/arc-big-query-pipeline-plugin package.
{{</note>}}

The `BigQueryExtract` stage reads directly from a BigQuery table and returns a `DataFrame`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|table|String|true|The BigQuery table in the format `[[project:]dataset.]table.`|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|dataset|String|false*|The dataset containing the table. Required if omitted in table.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|maxParallelism|Integer|false|The maximal number of partitions to split the data into.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|optimizedEmptyProjection|Boolean|false|The connector uses an optimized empty projection (select without any columns) logic, used for `count()` execution.<br><br>Default: `true`.|
|parentProject|String|false|The Google Cloud Project ID of the table to bill for the export. Defaults to the project of the Service Account being used.|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|project|String|false|The Google Cloud Project ID of the table. Defaults to the project of the Service Account being used.|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|String|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|
|viewMaterializationDataset|String|false|The dataset where the materialized view is going to be created. Defaults to view's dataset.|
|viewMaterializationProject|String|false|The Google Cloud Project ID where the materialized view is going to be created. Defaults to view's project id.|
|viewsEnabled|Boolean|false|Enables the connector to read from views and not only tables.<br><br>BigQuery views are not materialized by default, which means that the connector needs to materialize them before it can read them. `viewMaterializationProject` and `viewMaterializationDataset` can be used to provide view materialization options.<br><br>Default: `false`.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/BigQueryExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/BigQueryExtractComplete" highlight="json" >}}


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
|failMode|String|false|Either `permissive` or `failfast`:<br><br>`permissive` will create an empty dataframe of `[value, _filename]` in case of no files.<br><br>`failfast` will fail the Arc job if no files are found.<br><br>Default: `failfast`.|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/BytesExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/BytesExtractComplete" highlight="json" >}}


## CassandraExtract
##### Since: 2.0.0 - Supports Streaming: False
{{< note title="Plugin" >}}
The `CassandraExtract` is provided by the https://github.com/tripl-ai/arc-cassandra-pipeline-plugin package.
{{</note>}}

The `CassandraExtract` reads directly from a [Cassandra](https://cassandra.apache.org/) cluster and returns a `DataFrame`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|keyspace|String|true|The name of the Cassandra keyspace to extract from.|
|table|String|true|The name of the Cassandra table to extract from.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}} This also determines the maximum number of concurrent JDBC connections.|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}}. Any parameters provided will be added to the Cassandra connection object.|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/CassandraExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/CassandraExtractComplete" highlight="json" >}}


## DeltaLakeExtract
##### Since: 2.0.0 - Supports Streaming: True
{{< note title="Plugin" >}}
The `DeltaLakeExtract` is provided by the https://github.com/tripl-ai/arc-deltalake-pipeline-plugin package.
{{</note>}}

The `DeltaLakeExtract` stage reads one or more [DeltaLake](https://delta.io/) files and returns a `DataFrame`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI/Glob of the input Databricks Delta files.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|options|Map[String, String]|false|Time travel options to allow loading previous versions of the data. These values are limited to:<br><br>`versionAsOf` allows travelling to a specific version.<br><br>`timestampAsOf` allows travelling to the state before a specified timestamp.<br><br>`relativeVersion` allows travelling relative to the current version where the current version is `0` and `-1` is the previous version.|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|String|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|


### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/DeltaLakeExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/DeltaLakeExtractComplete" highlight="json" >}}


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
|basePath|URI|false|{{< readfile file="/content/partials/fields/basePath.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|delimiter|String|false|{{< readfile file="/content/partials/fields/delimiter.md" markdown="true" >}}|
|customDelimiter|String|true*|{{< readfile file="/content/partials/fields/customDelimiter.md" markdown="true" >}}|
|escape|String|false|A single character used for escaping quotes inside an already quoted value. Default: `\`.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|header|Boolean|false|Whether or not the dataset contains a header row. If available the output dataset will have named columns otherwise columns will be named `_col1`, `_col2` ... `_colN`.<br><br>Default: `false`.|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|multiLine|Boolean|false|Whether to load multiple lines as a single record or as individual records split by newline.<br><br>Default: `false`.|
|inputField|String|false|If using `inputView` this option allows you to specify the name of the field which contains the delimited data.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|quote|String|false|The type of quoting in the file. Supported values: `None`, `SingleQuote`, `DoubleQuote`.<br><br>Default: `DoubleQuote`.|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|String|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|
|watermark|Object|false|{{< readfile file="/content/partials/fields/watermark.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/DelimitedExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/DelimitedExtractComplete" highlight="json" >}}


## ElasticsearchExtract
##### Since: 1.9.0 - Supports Streaming: False
{{< note title="Plugin" >}}
The `ElasticsearchExtract` is provided by the https://github.com/tripl-ai/arc-elasticsearch-pipeline-plugin package.
{{</note>}}

The `ElasticsearchExtract` stage reads from an [Elasticsearch](https://www.elastic.co/products/elasticsearch) cluster and returns a `DataFrame`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|input|String|true|The name of the source Elasticsearch index.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Parameters for connecting to the [Elasticsearch](https://www.elastic.co/products/elasticsearch) cluster are detailed [here](https://www.elastic.co/guide/en/elasticsearch/hadoop/master/configuration.html).|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/ElasticsearchExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/ElasticsearchExtractComplete" highlight="json" >}}


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
|uriField|String|false|The name of a field containing the URI to send the request to. Only used if `inputView` specified. Takes precedence over `inputURI` if specified.|
|bodyField|String|false|The name of a field containing the request body/entity that is sent with a `POST` request. Only used if `inputView` specified. Takes precedence over `body` if specified.|
|body|String|false|The request body/entity that is sent with a `POST` request.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|headers|Map[String, String]|false|{{< readfile file="/content/partials/fields/headers.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|method|String|false|The request type with valid values `GET` or `POST`.<br><br>Default: `GET`.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
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
|basePath|URI|false|{{< readfile file="/content/partials/fields/basePath.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|dropInvalid|Boolean|false|Whether to drop any invalid image files.<br><br>Default: true.|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|watermark|Object|false|{{< readfile file="/content/partials/fields/watermark.md" markdown="true" >}}|

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
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}} This also determines the maximum number of concurrent JDBC connections.|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}}. Any parameters provided will be added to the JDBC connection object. These are not logged so it is safe to put passwords here.|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|partitionColumn|String|false|The name of a numeric column from the table in question which defines how to partition the table when reading in parallel from multiple workers. If set `numPartitions` must also be set.|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|predicates|Array[String]|false|{{< readfile file="/content/partials/fields/predicates.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|String|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/JDBCExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/JDBCExtractComplete" highlight="json" >}}


## JSONExtract
##### Since: 1.0.0 - Supports Streaming: True

The `JSONExtract` stage reads either one or more JSON files or an input `Dataset[String]` and returns a `DataFrame`.

If trying to run against an `inputView` in streaming mode this stage will not work. Instead try using the [from_json](https://spark.apache.org/docs/latest/api/sql/index.html#from_json) SQL Function with a [SQLTransform](../transform/#sqltransform).

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true*|Name of the incoming Spark dataset. If not present `inputURI` is requred.|
|inputURI|URI|true*|URI/Glob of the input `json` files. If not present `inputView` is requred.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|basePath|URI|false|{{< readfile file="/content/partials/fields/basePath.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|inputField|String|false|If using `inputView` this option allows you to specify the name of the field which contains the delimited data.|
|multiLine|Boolean|false|Whether the input directory contains a single JSON object per file or multiple JSON records in a single file, one per line (see [JSONLines](http://jsonlines.org/).<br><br>Default: true.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}<br><br>Additionally, by specifying the schema here, the underlying data source can skip the schema inference step, and thus speed up data loading.|
|schemaView|String|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|
|watermark|Object|false|{{< readfile file="/content/partials/fields/watermark.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/JSONExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/JSONExtractComplete" highlight="json" >}}


## KafkaExtract
##### Since: 1.0.8 - Supports Streaming: True
{{< note title="Plugin" >}}
The `KafkaExtract` is provided by the https://github.com/tripl-ai/arc-kafka-pipeline-plugin package.
{{</note>}}

The `KafkaExtract` stage reads records from a [Kafka](https://kafka.apache.org/) `topic` and returns a `DataFrame`. It requires a unique `groupID` to be set which on first run will consume from the `earliest` offset available in Kafka. Each subsequent run will use the offset as recorded against that `groupID`. This means that if a job fails before properly processing the data then data may need to be restarted from the earliest offset by creating a new `groupID`.

The returned `DataFrame` has the schema:

| Field | Type | Description |
|-------|------|-------------|
|`topic`|String|The Kafka Topic.|
|`partition`|Integer|The partition ID.|
|`offset`|Long|The record offset.|
|`timestamp`|Long|The record timestamp.|
|`timestampType`|Int|The record timestamp type.|
|`key`|Binary|The record key  as a byte array.|
|`value`|Binary|The record value as a byte array.|

Can be used in conjuction with [KafkaCommitExecute](../execute/#kafkacommitexecute) to allow quasi-transactional behaviour (with `autoCommit` set to `false`) - in that the offset commit can be deferred until certain dependent stages are sucessfully executed.

To convert the `key` or `value` from a Binary/byte array to a string it is possible to use the [decode](https://spark.apache.org/docs/latest/api/sql/index.html#decode) SQL Function with a [SQLTransform](../transform/#sqltransform) like:

```sql
SELECT
  CAST(key AS STRING) AS stringKey,
  CAST(value AS STRING) AS stringValue,
  ...
```

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|bootstrapServers|String|true|{{< readfile file="/content/partials/fields/bootstrapServers.md" markdown="true" >}}|
|topic|String|true|{{< readfile file="/content/partials/fields/topic.md" markdown="true" >}}|
|groupID|String|true|{{< readfile file="/content/partials/fields/groupID.md" markdown="true" >}}|
|autoCommit|Boolean|false|Whether to update the offsets in Kafka automatically. To be used in conjuction with [KafkaCommitExecute](../execute/#kafkacommitexecute) to allow quasi-transactional behaviour.<br><br>If `autoCommit` is set to `false` this stage will force `persist` equal to `true` so that Spark will not execute the Kafka extract process twice with a potentially different result (e.g. new messages added between extracts).<br><br>Default: `false`.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|maxPollRecords|Int|false|The maximum number of records returned in a single call to Kafka. Arc will then continue to poll until all records have been read.<br><br>Default: `500`.|
|maxRecords|Int|false|The maximum number of records returned in a single execution of this stage when executed in batch mode.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|strict|Boolean|false|Whether to perform record count validation. Will not work with compacted topics. Default: `true`.|
|timeout|Long|false|The time, in milliseconds, spent waiting in poll if data is not available in Kafka. Default: `10000`.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/KafkaExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/KafkaExtractComplete" highlight="json" >}}


## MetadataExtract
##### Since: 2.4.0 - Supports Streaming: True

The `MetadataExtract` stage extracts the metadata attached to an input `Dataframe` and returns a `DataFrame`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/MetadataExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/MetadataExtractComplete" highlight="json" >}}


## MongoDBExtract
##### Since: 2.0.0 - Supports Streaming: False
{{< note title="Plugin" >}}
The `MongoDBExtract` is provided by the https://github.com/tripl-ai/arc-mongo-pipeline-plugin package.
{{</note>}}

The `MongoDBExtract` stage reads a collection from [MongoDB](https://www.mongodb.com/) and returns a `DataFrame`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|options|Map[String, String]|false|Map of configuration parameters. These parameters are used to provide database connection/collection details.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|String|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/MongoDBExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/MongoDBExtractComplete" highlight="json" >}}


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
|basePath|URI|false|{{< readfile file="/content/partials/fields/basePath.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|String|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|
|watermark|Object|false|{{< readfile file="/content/partials/fields/watermark.md" markdown="true" >}}|

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
|basePath|URI|false|{{< readfile file="/content/partials/fields/basePath.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|String|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|
|watermark|Object|false|{{< readfile file="/content/partials/fields/watermark.md" markdown="true" >}}|

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
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|rampUpTime|Integer|false|How long to ramp up before the generating speed becomes rowsPerSecond. Using finer granularities than seconds will be truncated to integer seconds.<br><br>Default: 0.|
|rowsPerSecond|Integer|false|How many rows should be generated per second.<br><br>Default: 1.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/RateExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/RateExtractComplete" highlight="json" >}}


## SASExtract
##### Since: 2.4.0 - Supports Streaming: True

{{< note title="Plugin" >}}
The `SASExtract` is provided by the https://github.com/tripl-ai/arc-sas-pipeline-plugin package.
{{</note>}}

The `SASExtract` stage reads a collection from SAS `sas7bdat` binary file and returns a `DataFrame`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI/Glob of the input `sas7bdat` files.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|options|Map[String, String]|false|Options for reading the `sas7bdat` file. These values are limited to:<br><br>`inferDecimal`: infer numeric columns with format width  > 0 and format precision > 0, as `Decimal(Width, Precision)`.<br><br>`inferDecimalScale`: scale of inferred decimals.<br><br>`inferFloat`: infer numeric columns with <= 4 bytes, as `Float`.<br><br>`inferInt`: infer numeric columns with <= 4 bytes, format width > 0 and format precision =0, as `Int`.<br><br>`inferLong`: infer numeric columns with <= 8 bytes, format width > 0 and format precision = 0, as `Long`.<br><br>`inferShort`: infer numeric columns with <= 2 bytes, format width > 0 and format precision = 0, as `Short`.<br><br>`maxSplitSize`: maximum byte length of input splits which can be decreased to force higher parallelism.|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|String|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/SASExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/SASExtractComplete" highlight="json" >}}


## StatisticsExtract
##### Since: 3.5.0 - Supports Streaming: True

The `StatisticsExtract` stage extracts the column statistics from to an input `Dataframe` and returns a `DataFrame`.

It differs from the Spark inbuilt `summary` by:

- operates on all data types.
- returns row-based data rather than column-based (i.e. each input column is one row in the output)
- `count_distinct` and `null_count` additional metrics

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|approximate|Boolean|false|Whether to calculate `approximate` statistics or full `population` based statistics. Calculating `population` based statistics is a computationally and memory intenstive operation and may result in very long runtime or exceed memory limits.<br><br>Default: `true`.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|histogram|Boolean|false|Whether to calculate distribution statistics (`25%`, `50%`, `75%`).<br><br>Default: `false`.|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|hllRelativeSD|Double|false|The maximum relative standard deviation for the `distinct_count` output variable. Smaller values will provide greater precision at the expense of runtime.<br><br>Default: `0.05`.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/StatisticsExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/StatisticsExtractComplete" highlight="json" >}}


## TextExtract
##### Since: 1.2.0 - Supports Streaming: True

The `TextExtract` stage reads either one or more text files and returns a `DataFrame`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true*|Name of the incoming Spark dataset containing a list of URI/Globs to extract from.  If not present `inputURI` is requred.|
|inputURI|URI|true*|URI/Glob of the input text files. If not present `inputView` is requred.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|basePath|URI|false|{{< readfile file="/content/partials/fields/basePath.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|multiLine|Boolean|false|Whether to load the file as a single record or as individual records split by newline.<br><br>Default: `false`.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|schemaView|String|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|
|watermark|Object|false|{{< readfile file="/content/partials/fields/watermark.md" markdown="true" >}}|

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
|inputURI|URI|true*|URI/Glob of the input delimited XML files. If not present `inputView` is requred.|
|inputView|String|true*|Name of the incoming Spark dataset. If not present `inputURI` is requred.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|inputField|String|false|If using `inputView` this option allows you to specify the name of the field which contains the XML data.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}<br><br>Additionally, by specifying the schema here, the underlying data source can skip the schema inference step, and thus speed up data loading.|
|schemaView|String|false|{{< readfile file="/content/partials/fields/schemaView.md" markdown="true" >}}|
|xsdURI|URI|false|URI of an [XML Schema Definition](https://en.wikipedia.org/wiki/XML_Schema_(W3C)) (XSD) file used to validate input XML.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/XMLExtractMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/XMLExtractComplete" highlight="json" >}}