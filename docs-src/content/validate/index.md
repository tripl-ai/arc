---
title: Validate
weight: 60
type: blog
---

`*Validate` stages are used to perform validation and basic workflow controls..

## EqualityValidate
##### Since: 1.0.0 - Supports Streaming: False

The `EqualityValidate` takes two input `DataFrame` and will succeed if they are identical or fail if not. This stage is useful to use in automated testing as it can be used to validate a derived dataset equals a known 'good' dataset.

This stage will validate:

- Same number of columns.
- Same data type of columns.
- Same number of rows.
- Same values in those rows.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|leftView|String|true|Name of first incoming Spark dataset.|
|rightView|String|true|Name of second incoming Spark dataset.|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/EqualityValidateMin" highlight="json" >}} 


## MetadataValidate
##### Since: 2.4.0 - Supports Streaming: True

Similar to `SQLValidate`, the `MetadataValidate` stage takes an input SQL statement which is executed against the metadata attached to the input `DataFrame` which must return [Boolean, Option[String]] and will succeed if the first return value is true or fail if not.

Arc will register a table called `metadata` which contains the metadata of the `inputView`. This allows complex SQL statements to be executed which returns which columns to retain from the `inputView` in the `outputView`. The available columns in the `metadata` table are:

| Field | Description |
|-------|-------------|
|name|The field name.|
|type|The field type.|
|metadata|The field metadata.|

This can be used like:

```sql
-- ensure that no pii columns are present in the inputView
SELECT 
    SUM(pii_column) = 0
    ,TO_JSON(
        NAMED_STRUCT(
            'columns', COUNT(*), 
            'pii_columns', SUM(pii_column)
        )
    ) 
FROM (
    SELECT 
        CASE WHEN metadata.pii THEN 1 ELSE 0 END AS pii_column
    FROM detail
) valid
```

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputView|String|true|{{< readfile file="/content/partials/fields/inputView.md" markdown="true" >}}|
|inputURI|URI|true|{{< readfile file="/content/partials/fields/inputURI.md" markdown="true" >}}|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|sqlParams|Map[String, String]|false|{{< readfile file="/content/partials/fields/sqlParams.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/MetadataValidateMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/MetadataValidateComplete" highlight="json" >}} 


## SQLValidate
##### Since: 1.0.0 - Supports Streaming: False

The `SQLValidate` takes an input SQL statement which must return [Boolean, Option[String]] and will succeed if the first return value is true or fail if not. The second return value will be logged in case of success or failure which can be useful for understanding reason for job success/failure. This stage is exteremely powerful as abritrary job validation rules, expressed as SQL statements, can be executed to allow/prevent the job to succeed.

For example it can be used to perform automated extract validation against file formats which may have a header/footer layout or datasets where a certain level of data conversion errors are acceptable.

`SQLValidate` will try to convert the message from a JSON string which can be manually created in the SQL statement so that logging is easier to parse by log aggregation tools.

See [patterns](../patterns/) for more examples.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|inputURI|URI|*true|{{< readfile file="/content/partials/fields/inputURI.md" markdown="true" >}} Required if `sql` not provided.|
|sql|String|*true|{{< readfile file="/content/partials/fields/sql.md" markdown="true" >}} Required if `inputURI` not provided.|
|authentication|Map[String, String]|false|{{< readfile file="/content/partials/fields/authentication.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|sqlParams|Map[String, String]|false|{{< readfile file="/content/partials/fields/sqlParams.md" markdown="true" >}}|

### Examples

#### Minimal
{{< readfile file="/resources/docs_resources/SQLValidateMin" highlight="json" >}} 

#### Complete
{{< readfile file="/resources/docs_resources/SQLValidateComplete" highlight="json" >}} 

For example after performing a `TypingTransform` it would be possible to execute a query which tests that a certain percentage of records are not errored:

|_type|date|description|total|_error|
|----------|----|-----------|-----|-------|
|detail|2016-12-19|daily total|14.23|[false]|
|detail|2016-12-20|daily total|null|[true]|
|detail|2016-12-21|daily total|18.20|[false]|

With a `JSON` message (preferred):

```sql
SELECT 
    (SUM(errors) / COUNT(errors)) < ${record_error_tolerance_percentage}
    ,TO_JSON(NAMED_STRUCT('error', SUM(errors)/ COUNT(errors), 'threshold', ${record_error_tolerance_percentage})) 
FROM (
    SELECT 
        CASE WHEN SIZE(_errors) > 0 THEN 1 ELSE 0 END AS errors
    FROM detail
) valid
```

The `FILTER` function can be used to select errors only from a certain field: 

```sql
SIZE(FILTER(_errors, _error -> _error.field == 'fieldname'))
```

With a text message:

```sql
SELECT 
    (SUM(errors) / COUNT(errors)) < ${record_error_tolerance_percentage}
    ,CASE 
        WHEN (SUM(errors) / COUNT(errors)) < ${record_error_tolerance_percentage} THEN 'number of errors below threshold. success.'
        ELSE CONCAT('error records ', ROUND((SUM(errors) / COUNT(errors)) * 100, 2), '%. required < ', ${record_error_tolerance_percentage} * 100,'%') 
    END    
FROM (
    SELECT 
        CASE WHEN SIZE(_errors) > 0 THEN 1 ELSE 0 END AS errors
    FROM detail
) valid
```

