---
title: Metadata
weight: 70
type: blog
---

The `metadata` format, consumed in the [TypingTransform](../transform/#typingtransform) stage, is an opinionated format for specifying common data typing actions. 

It is designed to:

- Support common data typing conversions found in business datasets.
- Support limited 'schema evolution' of source data in the form of allowed lists of accepted input formats.
- Collect errors into array columns so that a user can decide how to handle errors once all have been collected.

## Common

### Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|id|String|true|{{< readfile file="/content/partials/fields/id.md" markdown="true" >}}|
|name|String|true|{{< readfile file="/content/partials/fields/fieldName.md" markdown="true" >}}|
|description|String|false|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|type|String|true|{{< readfile file="/content/partials/fields/description.md" markdown="true" >}}|
|trim|Boolean|true|{{< readfile file="/content/partials/fields/trim.md" markdown="true" >}}|
|nullable|Boolean|true|{{< readfile file="/content/partials/fields/nullable.md" markdown="true" >}}|
|nullableValues|Array[String]|false|{{< readfile file="/content/partials/fields/nullableValues.md" markdown="true" >}}|
|nullReplacementValue|String|false|{{< readfile file="/content/partials/fields/nullReplacementValue.md" markdown="true" >}}|
|metadata|Object|false|{{< readfile file="/content/partials/fields/metadata.md" markdown="true" >}}|

### Examples

```json
{
  "id" : "9712c383-22d1-44a6-9ca2-0087af4857f1",
  "name" : "first_name",
  "description" : "Customer First Name",
  "type" : "string",
  "trim" : true,
  "nullable" : true,
  "nullableValues" : [ "", "null" ],
  "metadata": {
    "primaryKey" : true,
    "position": 1
  }
}
```

## Boolean

### Additional Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|trueValues|Array[String]|true|{{< readfile file="/content/partials/fields/trueValues.md" markdown="true" >}}|
|falseValues|Array[String]|true|{{< readfile file="/content/partials/fields/falseValues.md" markdown="true" >}}|

### Examples

```json
{
  "id" : "982cbf60-7ba7-4e50-a09b-d8624a5c49e6",
  "name" : "marketing_opt_in_flag",
  "description" : "Whether the customer has opted in to receive marketing communications.",
  "type" : "boolean",
  "trim" : true,
  "nullable" : true,
  "nullableValues" : [ "", "null" ],
  "trueValues" : [ "true", "TRUE", "t", "1" ],
  "falseValues" : [ "false", "FALSE", "f", "0" ],
  "metadata": {
    "primaryKey" : true,
    "position": 1
  }  
}
```

## Date

{{< note title="Date vs Timestamp" >}}
This class does not store or represent a time or time-zone. Instead, it is a description of the date, as used for birthdays. It cannot represent an instant on the time-line without additional information such as an offset or time-zone. 

This means that if users will be executing SQL statements which have conditional logic based on date comparisons (such as `WHERE [date] < CURRENT_DATE()`) then it is safer to use a [Timestamp](#Timestamp) with a hard-coded time component for that source data so you get consistent results regardless of which time zone your users are located.
{{</note>}}

### Additional Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|formatters|Array[String]|true|{{< readfile file="/content/partials/fields/dateFormatters.md" markdown="true" >}}|

### Examples

```json
{
  "id" : "0e8109ba-1000-4b7d-8a4c-b01bae07027f",
  "name" : "birth_date",
  "description" : "Customer Birth Date",
  "type" : "date",
  "trim" : true,
  "nullable" : true,
  "nullableValues" : [ "", "null" ],
  "formatters" : [ "uuuuMMdd", "uuuu-MM-dd" ],
  "metadata": {
    "primaryKey" : true,
    "position": 1
  }
}
```

## Decimal

### Additional Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|precision|Integer|true|The total number of digits. e.g. 1234.567 has a precision of 7.|
|scale|Integer|true|The number of digits in the fraction part. e.g. 1234.567 has a scale of 3.|
|formatters|Array[String]|false|{{< readfile file="/content/partials/fields/numberFormatters.md" markdown="true" >}}<br><br>Default: `#,##0.###;-#,##0.###`|

### Examples

```json
{
  "id" : "9712c383-22d1-44a6-9ca2-0087af4857f1",
  "name" : "account_balance",
  "description" : "The current account balance",
  "type" : "decimal",
  "trim" : true,
  "nullable" : true,
  "nullableValues" : [ "", "null" ],
  "precision": 10,
  "scale": 2,
  "metadata": {
    "primaryKey" : true,
    "position": 1
  }  
}
```

## Double

A `Double` is a double-precision 64-bit IEEE 754 floating point number.

{{< note title="Double vs Decimal" >}}
A Decimal should be used whenever precision is required or for numbers which must sum up correctly or balance, e.g. monetary transactions. 
{{</note>}}

### Additional Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|formatters|Array[String]|false|{{< readfile file="/content/partials/fields/numberFormatters.md" markdown="true" >}}<br><br>Default: `#,##0.###;-#,##0.###`|

### Examples

```json
{
  "id" : "31541ea3-5b74-4753-857c-770bd601c35b",
  "name" : "last_meter_reading",
  "description" : "The last reading from the customer power meter.",
  "type" : "double",
  "trim" : true,
  "nullable" : true,
  "nullableValues" : [ "", "null" ],
  "metadata": {
    "primaryKey" : true,
    "position": 1
  }
}
```

## Integer

Use Integer when dealing with values up to ±2 billion (-2<sup>31</sup> to +2<sup>31</sup>-1)

### Additional Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|formatters|Array[String]|false|{{< readfile file="/content/partials/fields/numberFormatters.md" markdown="true" >}}<br><br>Default: `#,##0;-#,##0`|

### Examples

```json
{
  "id" : "a66f3bbe-d1c6-44c7-b096-a4be59fdcd78",
  "name" : "update_count",
  "description" : "Number of updates to this customer record.",
  "type" : "integer",
  "trim" : true,
  "nullable" : true,
  "nullableValues" : [ "", "null" ],
  "metadata": {
    "primaryKey" : true,
    "position": 1
  }
}
```

## Long

Use a Long Integer when dealing with values greater than ±2 billion (-2<sup>63</sup> to +2<sup>63</sup>-1)

### Additional Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|formatters|Array[String]|false|{{< readfile file="/content/partials/fields/numberFormatters.md" markdown="true" >}}<br><br>Default: `#,##0;-#,##0`|

### Examples

```json
{
  "id" : "1c0eec1d-17cd-45da-8744-7a9ef5b8b086",
  "name" : "transaction_num",
  "description" : "Global transaction sequence number.",
  "type" : "long",
  "trim" : true,
  "nullable" : true,
  "nullableValues" : [ "", "null" ],
  "metadata": {
    "primaryKey" : true,
    "position": 1
  }
}
```

## String

### Additional Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|length|Integer|false|The length of the string which can be used in data validation stages.|

### Examples

```json
{
  "id" : "9712c383-22d1-44a6-9ca2-0087af4857f1",
  "name" : "first_name",
  "description" : "Customer First Name",
  "type" : "string",
  "trim" : true,
  "nullable" : true,
  "nullableValues" : [ "", "null" ],
  "primaryKey" : false,
  "metadata": {
    "primaryKey" : true,
    "position": 1
  }
}
```

## Time

{{< note title="TimeType" >}}
Spark does not have an internal `TimeType` representation of time. This type can be used to ensure time values are able to be successfully parsed as [LocalTime](https://docs.oracle.com/javase/8/docs/api/java/time/LocalTime.html) objects but they are always stored in Spark as `string` formatted in the standard `HH:mm:ss` format type meaning they can safely used in the [to_utc_timestamp](https://spark.apache.org/docs/latest/api/sql/index.html#to_utc_timestamp) SQL function (but be very careful with timezone offsets). If they cannot be parsed then an error will be inserted into the `_errors` array like all other types.
{{</note>}}

### Additional Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|formatters|Array[String]|true|{{< readfile file="/content/partials/fields/dateFormatters.md" markdown="true" >}}|

### Examples

```json
{
  "id" : "0f5162ce-64ca-409d-abd1-f0b5bb5830de",
  "name" : "transaction_time",
  "description" : "Time of the database transaction",
  "type" : "time",
  "trim" : true,
  "nullable" : true,
  "nullableValues" : [ "", "null" ],
  "formatters" : [ "HHmmss" ],
  "metadata": {
    "primaryKey" : true,
    "position": 1
  }  
}
```

## Timestamp

### Additional Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|formatters|Array[String]|true|{{< readfile file="/content/partials/fields/dateFormatters.md" markdown="true" >}}|
|timezoneId|String|true|The timezone of the incoming timestamp. This uses the [SimpleDateFormat](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html#timezone) supported timezones. All timestamps are internally stored in [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time) to allow correctly sequenced events when dealing with events from multiple systems which may all run with different internal timezones.<br><br>Custom formats `ssssssssss` and `sssssssssssss` have been added to support epoch time (i.e. 1527727035) and epoch millis time (i.e. 1527727035456) respectively. Both require `timezoneId` of `UTC`.|
|time|Map[String, Integer]|false|Use this capability if converting a Date label into a Timestamp for relative comparisons. Required fields are `hour`, `minute`, `second` and `nano` . These values can be agreed with source data suppliers to ensure intra-system data alignment. See below for example.|

### Examples

```json
{
  "id" : "8e42c8f0-22a8-40db-9798-6dd533c1de36",
  "name" : "create_date",
  "description" : "Customer Creation Date",
  "type" : "timestamp",
  "trim" : true,
  "nullable" : true,
  "nullableValues" : [ "", "null" ],
  "formatters": [
    "dd/MM/uuuu HH:mm:ss",
    "dd/MM/uuuu H:mm:ss",
    "dd/MM/uuuu HH:mm",
    "dd/MM/uuuu H:mm",
    "d/MM/uuuu HH:mm:ss",
    "d/MM/uuuu H:mm:ss",
    "d/MM/uuuu HH:mm",
    "d/MM/uuuu H:mm"
  ],
  "timezoneId": "+1000",    
  "metadata": {
    "primaryKey" : true,
    "position": 1
  }
}
```

For converting a `Date` label into a `Timestamp` supply the `time` key:

```json
{
  "id" : "8e42c8f0-22a8-40db-9798-6dd533c1de36",
  "name" : "create_date",
  "description" : "Customer Creation Date",
  "type" : "timestamp",
  "trim" : true,
  "nullable" : true,
  "nullableValues" : [ "", "null" ],
  "formatters": [
    "dd/MM/uuuu",
  ],
  "timezoneId": "Australia/Sydney",    
  "time": {
    "hour": 23,
    "minute": 59,
    "second": 59,
    "nano": 0,
  },
  "metadata": {
    "primaryKey" : true,
    "position": 1
  }  
}
```