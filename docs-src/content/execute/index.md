---
title: Execute
weight: 50
type: blog
---

`*Execute` stages are used to execute arbitrary commands against external systems such as Databases and APIs.

## BigQueryExecute
##### Supports Streaming: False
{{< note title="Plugin" >}}
The `BigQueryExecute` is provided by the https://github.com/tripl-ai/arc-big-query-pipeline-plugin package.
{{</note>}}

The `BigQueryExecute` executes a SQL statement against BigQuery.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|*true|{{< readfile file="/content/partials/fields/inputURI.md" markdown="true" >}} Required if `sql` not provided.|
|sql|String|*true|{{< readfile file="/content/partials/fields/sql.md" markdown="true" >}} Required if `inputURI` not provided.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|jobName|String|false|BigQuery Job name useful for identifying events in log messages.|
|location|String|false|Location in which to invoke the BigQuery job.|
|sqlParams|Map[String, String]|false|{{< readfile file="/content/partials/fields/sqlParams.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/BigQueryExecuteMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/BigQueryExecuteComplete" highlight="json" >}}


## CassandraExecute
##### Since: 2.0.0 - Supports Streaming: False
{{< note title="Plugin" >}}
The `CassandraExecute` is provided by the https://github.com/tripl-ai/arc-cassandra-pipeline-plugin package.
{{</note>}}

The `CassandraExecute` executes a CQL statement against an external [Cassandra](https://cassandra.apache.org/) cluster.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI of the input file containing the CQL statement.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}}. Any parameters provided will be added to the Cassandra connection object.|
|sqlParams|Map[String, String]|false|{{< readfile file="/content/partials/fields/sqlParams.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/CassandraExecuteMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources_plugins/CassandraExecuteComplete" highlight="json" >}}


## ConfigExecute
##### Since: 3.4.0 - Supports Streaming: True

The `ConfigExecute` takes an input SQL statement which must return a `string` formatted `JSON` object allowing runtime creation of job configuration substitution values. `ConfigExecute` is intended to be used with [Dynamic Variables](deploy/#dynamic-variables) to allow the creation of variables reliant on runtime data.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|*true|{{< readfile file="/content/partials/fields/inputURI.md" markdown="true" >}} Required if `sql` not provided.|
|sql|String|*true|{{< readfile file="/content/partials/fields/sql.md" markdown="true" >}} Required if `inputURI` not provided.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|persist|Boolean|false|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|sqlParams|Map[String, String]|false|{{< readfile file="/content/partials/fields/sqlParams.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/ConfigExecuteMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/ConfigExecuteComplete" highlight="json" >}}


## HTTPExecute
##### Since: 1.0.0 - Supports Streaming: False

The `HTTPExecute` takes an input `Map[String, String]` from the configuration and executes a `POST` request against a remote HTTP service. This could be used to initialise another process that depends on the output of data pipeline.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|uri|URI|true|URI of the HTTP server.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|headers|Map[String, String]|false|{{< readfile file="/content/partials/fields/headers.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|payloads|Map[String, String]|false|{{< readfile file="/content/partials/fields/payloads.md" markdown="true" >}}|
|validStatusCodes|Array[Integer]|false|{{< readfile file="/content/partials/fields/validStatusCodes.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/HTTPExecuteMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/HTTPExecuteComplete" highlight="json" >}}


## JDBCExecute
##### Since: 1.0.0 - Supports Streaming: False

The `JDBCExecute` executes a SQL statement against an external JDBC connection.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|{{< readfile file="/content/partials/fields/inputURI.md" markdown="true" >}}|
|jdbcURL|String|true|{{< readfile file="/content/partials/fields/jdbcURL.md" markdown="true" >}} You may be required to set `allowMultiQueries=true` in the connection string to execute multiple statements.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}}. Any parameters provided will be added to the JDBC connection object. These are not logged so it is safe to put passwords here.|
|sqlParams|Map[String, String]|false|{{< readfile file="/content/partials/fields/sqlParams.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/JDBCExecuteMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/JDBCExecuteComplete" highlight="json" >}}


## KafkaCommitExecute
##### Since: 1.0.8 - Supports Streaming: False
{{< note title="Plugin" >}}
The `KafkaCommitExecute` is provided by the https://github.com/tripl-ai/arc-kafka-pipeline-plugin package.
{{</note>}}

The `KafkaCommitExecute` takes the resulting `DataFrame` from a [KafkaExtract](../extract/#kafkaextract) stage and commits the offsets back to Kafka. This is used so that a user is able to perform a quasi-transaction by specifing a series of stages that must be succesfully executed prior to `committing` the offset back to Kafka. To use this stage ensure that the `autoCommit` option on the [KafkaExtract](../extract/#kafkaextract) stage is set to `false`.

For example, if a job reads from a Kafka topic and writes the results to `parquet` then it would be good to ensure the [ParquetLoad](../load/#parquetload) stage had completed successfully before updating the offset in Kafka.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|bootstrapServers|String|true|{{< readfile file="/content/partials/fields/bootstrapServers.md" markdown="true" >}}|
|groupID|String|true|{{< readfile file="/content/partials/fields/groupID.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources_plugins/KafkaCommitExecuteMin" highlight="json" >}}


## LogExecute
##### Since: 2.12.0 - Supports Streaming: True

The `LogExecute` takes an input SQL statement which must return a `string` and will write the output to the Arc logs. `LogExecute` will try to convert the message from a JSON string manually created in the SQL statement so that logging is easier to parse by log aggregation tools.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|*true|{{< readfile file="/content/partials/fields/inputURI.md" markdown="true" >}} Required if `sql` not provided.|
|sql|String|*true|{{< readfile file="/content/partials/fields/sql.md" markdown="true" >}} Required if `inputURI` not provided.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|
|sqlParams|Map[String, String]|false|{{< readfile file="/content/partials/fields/sqlParams.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/LogExecuteMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/LogExecuteComplete" highlight="json" >}}


## PipelineExecute
##### Since: 1.0.9 - Supports Streaming: True

The `PipelineExecute` stage allows the embedding of another Arc pipeline within the current pipeline. This means it is possible to compose pipelines together without having to [serialise](../load) and [deserialise](../extract) the results.

An example use case could be a `pipeline` which defines how your organisation defines active customer records which could then be embedded in multiple downstream `pipelines` to ensure definition consistency.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|uri|String|true|URI of the input file containing the definition of the `pipeline` to include.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|id|String|false|{{< readfile file="/content/partials/fields/stageId.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/PipelineExecuteMin" highlight="json" >}}

#### Complete
{{< readfile file="/resources/docs_resources/PipelineExecuteComplete" highlight="json" >}}