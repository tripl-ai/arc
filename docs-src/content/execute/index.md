---
title: Execute
weight: 50
type: blog
---

`*Execute` stages are used to execute arbitrary commands against external systems such as Databases and APIs.

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
|jdbcURL|String|true|{{< readfile file="/content/partials/fields/jdbcURL.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} All parameters will be added to the Connection Properties.|
|password|String|false|Database password for the given user. Optional, can also be in the url or params.|
|sqlParams|Map[String, String]|false|{{< readfile file="/content/partials/fields/sqlParams.md" markdown="true" >}}|
|user|String|false|Database username to connect as. Optional, can also be in the url or params.|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/JDBCExecuteMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/JDBCExecuteComplete" highlight="json" >}} 


## KafkaCommitExecute
##### Since: 1.0.8 - Supports Streaming: False

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

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/KafkaCommitExecuteMin" highlight="json" >}} 

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

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/PipelineExecuteMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/PipelineExecuteComplete" highlight="json" >}} 