---
title: Execute
weight: 50
type: blog
---

`*Execute` stages are used to execute arbitrary commands against external systems such as Databases and APIs.

## HTTPExecute

The `HTTPExecute` takes an input `Map[String, String]` from the configuration and executes a `POST` request against a remote HTTP service. This could be used to initialise another process that depends on the output of data pipeline.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputURI|URI|true|URI of the HTTP server.|
|headers|Map[String, String]|false|{{< readfile file="/content/partials/fields/headers.md" markdown="true" >}}|
|payloads|Map[String, String]|false|{{< readfile file="/content/partials/fields/payloads.md" markdown="true" >}}|
|validStatusCodes|Array[Integer]|false|{{< readfile file="/content/partials/fields/validStatusCodes.md" markdown="true" >}}|
|params|Map[String, String]|true|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "HTTPExecute",
    "name": "informs job manager of job completion",
    "environments": ["production", "test"],
    "inputView": "customer",            
    "outputURI": "http://internalserver/api/job",
    "payloads": {
        "jobName": "customer",
        "jobStatus": "complete",
    },    
    "headers": {
        "Authorization": "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==",
        "custom-header": "payload",
    },
    "validStatusCodes": [200],
    "params": {
    }
}
```

## JDBCExecute

The `JDBCExecute` executes a SQL statement against an external JDBC connection.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|{{< readfile file="/content/partials/fields/inputURI.md" markdown="true" >}}|
|url|String|true|{{< readfile file="/content/partials/fields/jdbcURL.md" markdown="true" >}}|
|user|String|false|Database username to connect as. Optional, can also be in the url or params.|
|password|String|false|Database password for the given user. Optional, can also be in the url or params.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|sqlParams|Map[String, String]|false|{{< readfile file="/content/partials/fields/sqlParams.md" markdown="true" >}}|
|params|Map[String, String]|true|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}}. All params will be added to the Connection Properties.|

### Examples

```json
{
    "type": "JDBCExecute",
    "name": "update the load date table",
    "environments": ["production", "test"],
    "inputURI": "hdfs://datalake/sql/update_customer_load_date.sql",          
    "url": "jdbc:postgresql://localhost/test",
    "user": "test",
    "password": "test",
    "sqlParams": {
        "current_timestamp": "2018-11-24 14:48:56"
    },    
    "params": {
    }
}
```