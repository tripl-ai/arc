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

## AvroExtract
##### Since: 1.0.0

The `AvroExtract` stage reads one or more [Apache Avro](https://avro.apache.org/) files and returns a `DataFrame`. 

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI of the input Avro files.|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "AvroExtract",
    "name": "load customer avro extract",
    "environments": ["production", "test"],
    "inputURI": "hdfs://input_data/customer/*.avro",
    "outputView": "customer",            
    "persist": false,
    "authentication": {
        ...
    },    
    "params": {
    }
}
```

## DelimitedExtract
##### Since: 1.0.0

The `DelimitedExtract` stage reads either one or more delimited text files or an input `Dataset[String]` and returns a `DataFrame`. `DelimitedExtract` will always set the underlying Spark configuration option of `inferSchema` to `false` to ensure consistent results.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|false*|Name of the incoming Spark dataset. If not present `inputURI` is requred.|
|inputURI|URI|false*|URI of the input delimited text files. If not present `inputView` is requred.|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|delimiter|String|true|The type of delimiter in the file. Supported values: `Comma`, `Pipe`, `DefaultHive`. `DefaultHive` is  ASCII character 1, the default delimiter for Apache Hive extracts.|
|quote|String|true|The type of quoting in the file. Supported values: `None`, `SingleQuote`, `DoubleQuote`.|
|header|Boolean|true|Whether or not the dataset contains a header row. If available the output dataset will have named columns otherwise columns will be named `_col1`, `_col2` ... `_colN`.|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "DelimitedExtract",
    "name": "load customer csv extract",
    "environments": ["production", "test"],
    "inputURI": "hdfs://input_data/customer/*.csv",
    "outputView": "customer",            
    "persist": false,
    "delimiter": "Comma",
    "quote" : "DoubleQuote",
    "header": true,
    "authentication": {
        ...
    },
    "params": {
    }
}
```

```json
{
    "type": "DelimitedExtract",
    "name": "split customer record extract",
    "environments": ["production", "test"],
    "inputView": "customer_raw",
    "outputView": "customer",            
    "persist": false,
    "delimiter": "DefaultHive",
    "quote" : "SingleQuote",
    "header": false,
    "authentication": {
        ...
    },    
    "params": {
    }
}
```

## HTTPExtract
##### Since: 1.0.0

The `HTTPExtract` executes either a `GET` or `POST` request against a remote HTTP service and returns a `DataFrame` which will have a single row and single column holding the value of the HTTP response body. 

This stage would typically be used with a `JSONExtract` stage by specifying `inputView` instead of `inputURI` (setting `multiLine`=`true` allows processing of JSON array responses).

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI of the HTTP server.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|method|String|false|The request type with valid values `GET` or `POST`.<br><br>Default: `GET`.|
|headers|Map[String, String]|false|{{< readfile file="/content/partials/fields/headers.md" markdown="true" >}}|
|body|String|false|The request body/entity that is sent with a `POST` request.|
|validStatusCodes|Array[Integer]|false|{{< readfile file="/content/partials/fields/validStatusCodes.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "HTTPExtract",
    "name": "load customer from customer api",
    "environments": ["production", "test"],
    "inputURI": "http://internalserver/api/customer",
    "outputView": "customer",            
    "persist": false,
    "headers": {
        "Authorization": "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==",
        "custom-header": "payload",
    },
    "validStatusCodes": [200],
    "method": "GET",
    "params": {
    }
}
```

## JDBCExtract
##### Since: 1.0.0

The `JDBCExtract` reads directly from a JDBC Database and returns a `DataFrame`. See [Spark JDBC documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases).

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|jdbcURL|String|true|{{< readfile file="/content/partials/fields/jdbcURL.md" markdown="true" >}}|
|tableName|String|true|{{< readfile file="/content/partials/fields/tableName.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}} This also determines the maximum number of concurrent JDBC connections.|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|fetchsize|Integer|false|{{< readfile file="/content/partials/fields/fetchsize.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}}. Currently requires `user` and `password` to be set here - see example below.|

### Examples

```json
{
    "type": "JDBCExtract",
    "name": "extract customer from jdbc",
    "environments": ["production", "test"],
    "outputView": "ative_customers",            
    "persist": false,
    "jdbcURL": "jdbc:mysql://localhost/mydb",
    "tableName": "(SELECT * FROM customer WHERE active=TRUE) customer",
    "numPartitions": 10,
    "fetchsize": 1000,
    "params": {
        "user": "mydbuser",
        "password": "mydbpassword",
    }
}
```

## JSONExtract
##### Since: 1.0.0

The `JSONExtract` stage reads either one or more JSON files or an input `Dataset[String]` and returns a `DataFrame`. 

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|false*|Name of the incoming Spark dataset. If not present `inputURI` is requred.|
|inputURI|URI|false*|URI of the input delimited text files. If not present `inputView` is requred.|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}<br><br>Additionally, by specifying the schema here, the underlying data source can skip the schema inference step, and thus speed up data loading.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|multiLine|Boolean|false|Whether the input directory contains a single JSON object per file or multiple JSON records in a single file, one per line (see [JSONLines](http://jsonlines.org/).<br><br>Default: true.|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "JSONExtract",
    "name": "load customer json extract",
    "environments": ["production", "test"],
    "inputURI": "hdfs://input_data/customer/*.json",
    "outputView": "customer",            
    "persist": false,
    "multiLine": true,
    "authentication": {
        ...
    },
    "params": {
    }
}
```

## KafkaExtract
##### Since: 1.0.8

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
|topic|String|true|{{< readfile file="/content/partials/fields/topic.md" markdown="true" >}}|
|bootstrapServers|String|true|{{< readfile file="/content/partials/fields/bootstrapServers.md" markdown="true" >}}|
|groupID|String|true|{{< readfile file="/content/partials/fields/groupID.md" markdown="true" >}}|
|maxPollRecords|Int|false|The maximum number of records returned in a single call to Kafka. Arc will then continue to poll until all records have been read.<br><br>Default: 10000.|
|timeout|Long|false|The time, in milliseconds, spent waiting in poll if data is not available in Kafka. Default: 10000.|
|autoCommit|Boolean|false|Whether to update the offsets in Kafka automatically. To be used in conjuction with [KafkaCommitExecute](../execute/#kafkacommitexecute) to allow quasi-transactional behaviour.<br><br>If `autoCommit` is set to `false` this stage will force `persist` equal to `true` so that Spark will not execute the Kafka extract process twice with a potentially different result (e.g. new messages added between extracts).<br><br>Default: false.|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "KafkaExtract",
    "name": "read customer records from kafka",
    "environments": ["production", "test"],
    "outputView": "customer",
    "topic": "customer", 
    "bootstrapServers": "kafka:29092", 
    "groupID": "spark-customer-extract-job",
    "maxPollRecords": 10000,
    "timeout": 0,
    "autoCommit": false, 
    "persist": true,
    "params": {}
}
```

## ORCExtract
##### Since: 1.0.0

The `ORCExtract` stage reads one or more [Apache ORC](https://orc.apache.org/) files and returns a `DataFrame`. 

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI of the input ORC files.|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "ParquetExtract",
    "name": "load customer orc extract",
    "environments": ["production", "test"],
    "inputURI": "hdfs://input_data/customer/*.orc",
    "outputView": "customer",            
    "persist": false,
    "authentication": {
        ...
    },    
    "params": {
    }
}
```

## ParquetExtract
##### Since: 1.0.0

The `ParquetExtract` stage reads one or more [Apache Parquet](https://parquet.apache.org/) files and returns a `DataFrame`. 

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI of the input Parquet files.|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "ParquetExtract",
    "name": "load customer parquet extract",
    "environments": ["production", "test"],
    "inputURI": "hdfs://input_data/customer/*.parquet",
    "outputView": "customer",            
    "persist": false,
    "authentication": {
        ...
    },    
    "params": {
    }
}
```

## XMLExtract
##### Since: 1.0.0

The `XMLExtract` stage reads one or more XML files and returns a `DataFrame`. 

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI of the input XML files.|
|schemaURI|URI|false|{{< readfile file="/content/partials/fields/schemaURI.md" markdown="true" >}}<br><br>Additionally, by specifying the schema here, the underlying data source can skip the schema inference step, and thus speed up data loading.|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|numPartitions|Integer|false|{{< readfile file="/content/partials/fields/numPartitions.md" markdown="true" >}}|
|partitionBy|Array[String]|false|{{< readfile file="/content/partials/fields/partitionBy.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|rowTag|String|true|{{< readfile file="/content/partials/fields/rowTag.md" markdown="true" >}}|
|contiguousIndex|Boolean|false|{{< readfile file="/content/partials/fields/contiguousIndex.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "XMLExtract",
    "name": "load customer xml extract",
    "environments": ["production", "test"],
    "inputURI": "hdfs://input_data/customer/*.xml",
    "outputView": "customer",            
    "persist": false,
    "rowTag": "customer",
    "authentication": {
        ...
    },    
    "params": {
    }
}
```