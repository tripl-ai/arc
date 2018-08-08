---
title: Transform
weight: 30
type: blog
---

`*Transform` stages apply a single transformation to one or more incoming datasets.

Transformers should meet this criteria:

- Be [pure](https://en.wikipedia.org/wiki/Pure_function).
- Perform only a [single function](https://en.wikipedia.org/wiki/Separation_of_concerns).
- Utilise Spark [internal functionality](https://spark.apache.org/docs/latest/sql-programming-guide.html) where possible.

## DiffTransform

The `DiffTransform` stage calculates the difference between two input datasets and produces three datasets: 

- A dataset of the `intersection` of the two datasets - or rows that exist and are the same in both datasets.
- A dataset of the `left` dataset - or rows that only exist in the left input dataset (`inputLeftView`).
- A dataset of the `right` dataset - or rows that only exist in the right input dataset (`inputRightView`).

{{< note title="Persistence" >}}
This stage performs this 'diffing' operation in a single pass so if multiple of the output views are going to be used then it is a good idea to set persist = `true` to reduce the cost of recomputing the difference multiple times.
{{</note>}}

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|inputLeftView|String|true|Name of first incoming Spark dataset.|
|inputRightView|String|true|Name of second incoming Spark dataset.|
|outputIntersectionView|String|false|Name of output `intersection` view.|
|outputLeftView|String|false|Name of output `left` view.|
|outputRightView|String|false|Name of output `right` view.|
|persist|Boolean|true|Whether to persist dataset to Spark cache.|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "DiffTransform",
    "name": "calculate the difference between the yesterday and today datasets",
    "environments": ["production", "test"],
    "inputLeftView": "cutomer_20180501",            
    "inputRightView": "cutomer_20180502",            
    "outputIntersectionView": "customer_unchanged",            
    "outputLeftView": "customer_removed",            
    "outputRightView": "customer_added",            
    "persist": true,
    "params": {
    }
}
```

## JSONTransform

The `JSONTransform` stage transforms the incoming dataset to rows of `json` strings with the column name `value`. It is intended to be used before stages like [HTTPLoad](/load/#httpload) to prepare the data for sending externally. 

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "JSONTransform",
    "name": "convert customer data to json",
    "environments": ["production", "test"],
    "inputView": "cutomers",            
    "outputView": "customersJSON",            
    "persist": false,
    "params": {
    }
}
```

## MLTransform

The `MLTransform` stage transforms the incoming dataset with a pretrained Spark ML (Machine Learning) model. This will append one or more predicted columns to the incoming dataset. The incoming model must be a `PipelineModel` or `CrossValidatorModel` produced using Spark's Scala, Java, PySpark or SparkR API.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI of the input `PipelineModel` or `CrossValidatorModel`.|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}} MLTransform will also log percentiles of prediction probabilities for classification models if this option is enabled.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "MLTransform",
    "name": "apply machine learning model",
    "environments": ["production", "test"],
    "inputURI": "hdfs://input_data/ml/machineLearningPipelineModel.parquet",
    "inputView": "inputDF",         
    "outputView": "outputDF",            
    "persist": false,
    "authentication": {
        ...
    },
    "params": {
    }
}
```

## SQLTransform

The `SQLTransform` stage transforms the incoming dataset with a [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) statement. This stage relies on previous stages to load and register the dataset views (`outputView`) and will execute arbitrary SQL statements against those datasets.

{{< note title="CAST vs TypingTransform" >}}
It is strongly recommended to use the `TypingTransform` for reproducible, repeatable results.

Whilst SQL is capable of converting data types using the `CAST` function (e.g. `CAST(dateColumn AS DATE)`) be very careful. ANSI SQL specifies that any failure to convert then an exception condition is raised: `data exception-invalid character value for cast` whereas Spark SQL will return a null value and suppress any exceptions: `try s.toString.toInt catch { case _: NumberFormatException => null }`. If you used a cast in a financial scenario, for example bill aggregation, the silent `NULL`ing of values could result in errors being suppressed and bills incorrectly calculated.
{{</note>}}

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|{{< readfile file="/content/partials/fields/inputURI.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|sqlParams|Map[String, String]|false|{{< readfile file="/content/partials/fields/sqlParams.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "SQLTransform",
    "name": "Join customer and account",
    "environments": ["production", "test"],
    "inputURI": "hdfs://datalake/sql/0.0.1/customerAccountJoin.sql",
    "outputView": "customerAccountDF",            
    "persist": false,
    "authentication": {
        ...
    },    
    "sqlParams": {
        "current_date": "2018-11-24",
        "current_timestamp": "2018-11-24 14:48:56"
    },    
    "params": {
    }
}
```

The `current_date` and `current_timestamp` can easily be passed in as environment variables using `$(date "+%Y-%m-%d")` and `$(date "+%Y-%m-%d %H:%M:%S")` respectively.

The SQL statement is a plain Spark SQL statement, for example:

```sql
SELECT 
    customer.customer_id
    ,customer.first_name
    ,customer.last_name
    ,account.account_id
    ,account.account_name
FROM customer
LEFT JOIN account ON account.customer_id = customer.customer_id
```

## TensorFlowServingTransform

{{< note title="Experimental" >}}
The `TensorFlowServingTransform` is currently in experimental state whilst the requirements become clearer. 

This means this API is likely to change.
{{</note>}}

The `TensorFlowServingTransform` stage transforms the incoming dataset by calling a [TensorFlow Serving](https://www.tensorflow.org/serving/) service. Because each call is atomic the TensorFlow Serving instances could be behind a load balancer to increase throughput.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|inputFields|Map[String, String]|true|{{< readfile file="/content/partials/fields/inputFields.md" markdown="true" >}}|
|outputFields|Map[String, String]|true|{{< readfile file="/content/partials/fields/outputFields.md" markdown="true" >}}|
|hostname|String|true|{{< readfile file="/content/partials/fields/hostname.md" markdown="true" >}}|
|port|Integer|true|{{< readfile file="/content/partials/fields/port.md" markdown="true" >}}|
|modelName|String|true|{{< readfile file="/content/partials/fields/modelName.md" markdown="true" >}}|
|signatureName|String|true|{{< readfile file="/content/partials/fields/signatureName.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "environments": ["prd","tst"],
    "type": "TensorFlowServingTransform",
    "name": "call the customer segmentation model",
    "inputView": "customer",
    "outputView": "customer_segmented",            
    "inputFields": {
        "customer_usage": "DT_DOUBLE"
    },
    "outputFields": {
        "customer_segment": "DT_INT32"
    },
    "hostname": "tf",
    "port": 9000,
    "modelName": "simple", 
    "signatureName": "serving_default",
    "persist": true,
    "params": {}
}   
```

## TypingTransform

The `TypingTransform` stage transforms the incoming dataset with based on metadata defined in the [metadata](../metadata/) format. 

The logical process that is applied to perform the typing on a field-by-field basis is shown below.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|true|URI of the input file containing the SQL statement.|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|outputView|String|true|{{< readfile file="/content/partials/fields/outputView.md" markdown="true" >}}|
|persist|Boolean|true|{{< readfile file="/content/partials/fields/persist.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|params|Map[String, String]|false|{{< readfile file="/content/partials/fields/params.md" markdown="true" >}} Currently unused.|

### Examples

```json
{
    "type": "TypingTransform",
    "name": "apply data types to customer records",
    "environments": ["production", "test"],
    "inputURI": "hdfs://datalake/meta/0.0.1/customer_meta.json",
    "inputView": "customerUntypedDF",            
    "outputView": "customerTypeDF",            
    "persist": false,
    "authentication": {
        ...
    },       
    "params": {
    }
}
```

### Logical Flow

The sequence that these fields are converted from `string` fields to `typed` fields is per this flow chart. Each value and its typing metadata is passed into this logical process. For each row the `values` are returned as standard table columns and the returned `error` values are groupd into a field called `_errors` on a row-by-row basis. Patterns for consuming the `_errors` array is are demonstrated in the [SQLValidate](../validate/#sqlvalidate) stage.

![Logical Flow for Data Typing](/img/typing_flow.png "Logical Flow for Data Typing")
