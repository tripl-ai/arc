---
title: Schema
weight: 70
type: blog
---

The `schema` format, consumed in the [TypingTransform](../transform/#typingtransform) and other stages, is an opinionated format for specifying common data typing actions.

It is designed to:

- Allow precise definition of how to perform common data typing conversions found in business datasets.
- Support limited [Schema Evolution](https://en.wikipedia.org/wiki/Schema_evolution) of source data in the form of allowed lists of accepted input formats.
- Specification of metadata to attach to columns.

## Common

### Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|id|String|false|{{< readfile file="/content/partials/fields/id.md" markdown="true" >}}|
|name|String|true|{{< readfile file="/content/partials/fields/fieldName.md" markdown="true" >}}|
|description|String|false|A description of the field which will be embedded in the dataset metadata (and persisted in formats like Parquet/ORC).|
|type|String|true|{{< readfile file="/content/partials/fields/type.md" markdown="true" >}}|
|metadata|Object|false|{{< readfile file="/content/partials/fields/metadata.md" markdown="true" >}}|

### Examples

```json
{
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

## Array

An `array` defines a schema for a repeated list of elements. It requires the specification of the [schema](../schema/) of the nested elements which must all be of the same type.

### Additional Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|elementType|[schema](../schema/)|true|The [schema](../schema/) of the nested list of elements.|
|nullable|Boolean|true|{{< readfile file="/content/partials/fields/nullable.md" markdown="true" >}}|

### Examples

```json
{
  "name" : "customer_phone_numbers",
  "description" : "Customer Phone Numbers",
  "type" : "array",
  "metadata": {},
  "elementType": {
    "name" : "phone_number",
    "description" : "Phone Number",
    "type" : "string",
    "trim" : true,
    "nullable" : true,
    "nullableValues" : [ "", "null" ]
  }
}
```

## Binary

### Additional Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|encoding|String|true|The binary-to-text encoding format of the value. Valid values `base64`, `hexadecimal`.|
|trim|Boolean|true|{{< readfile file="/content/partials/fields/trim.md" markdown="true" >}}|
|nullable|Boolean|true|{{< readfile file="/content/partials/fields/nullable.md" markdown="true" >}}|
|nullableValues|Array[String]|false|{{< readfile file="/content/partials/fields/nullableValues.md" markdown="true" >}}|
|nullReplacementValue|String|false|{{< readfile file="/content/partials/fields/nullReplacementValue.md" markdown="true" >}}|

### Examples

```json
{
  "name" : "id",
  "description" : "GUID identifier",
  "type" : "binary",
  "trim" : true,
  "nullable" : true,
  "nullableValues" : [ "", "null" ],
  "encoding" : "base64",
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
|trim|Boolean|true|{{< readfile file="/content/partials/fields/trim.md" markdown="true" >}}|
|nullable|Boolean|true|{{< readfile file="/content/partials/fields/nullable.md" markdown="true" >}}|
|nullableValues|Array[String]|false|{{< readfile file="/content/partials/fields/nullableValues.md" markdown="true" >}}|
|nullReplacementValue|String|false|{{< readfile file="/content/partials/fields/nullReplacementValue.md" markdown="true" >}}|

### Examples

```json
{
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
|trim|Boolean|true|{{< readfile file="/content/partials/fields/trim.md" markdown="true" >}}|
|nullable|Boolean|true|{{< readfile file="/content/partials/fields/nullable.md" markdown="true" >}}|
|nullableValues|Array[String]|false|{{< readfile file="/content/partials/fields/nullableValues.md" markdown="true" >}}|
|nullReplacementValue|String|false|{{< readfile file="/content/partials/fields/nullReplacementValue.md" markdown="true" >}}|

### Examples

```json
{
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
|trim|Boolean|true|{{< readfile file="/content/partials/fields/trim.md" markdown="true" >}}|
|nullable|Boolean|true|{{< readfile file="/content/partials/fields/nullable.md" markdown="true" >}}|
|nullableValues|Array[String]|false|{{< readfile file="/content/partials/fields/nullableValues.md" markdown="true" >}}|
|nullReplacementValue|String|false|{{< readfile file="/content/partials/fields/nullReplacementValue.md" markdown="true" >}}|

### Examples

```json
{
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
|trim|Boolean|true|{{< readfile file="/content/partials/fields/trim.md" markdown="true" >}}|
|nullable|Boolean|true|{{< readfile file="/content/partials/fields/nullable.md" markdown="true" >}}|
|nullableValues|Array[String]|false|{{< readfile file="/content/partials/fields/nullableValues.md" markdown="true" >}}|
|nullReplacementValue|String|false|{{< readfile file="/content/partials/fields/nullReplacementValue.md" markdown="true" >}}|

### Examples

```json
{
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
|trim|Boolean|true|{{< readfile file="/content/partials/fields/trim.md" markdown="true" >}}|
|nullable|Boolean|true|{{< readfile file="/content/partials/fields/nullable.md" markdown="true" >}}|
|nullableValues|Array[String]|false|{{< readfile file="/content/partials/fields/nullableValues.md" markdown="true" >}}|
|nullReplacementValue|String|false|{{< readfile file="/content/partials/fields/nullReplacementValue.md" markdown="true" >}}|

### Examples

```json
{
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
|trim|Boolean|true|{{< readfile file="/content/partials/fields/trim.md" markdown="true" >}}|
|nullable|Boolean|true|{{< readfile file="/content/partials/fields/nullable.md" markdown="true" >}}|
|nullableValues|Array[String]|false|{{< readfile file="/content/partials/fields/nullableValues.md" markdown="true" >}}|
|nullReplacementValue|String|false|{{< readfile file="/content/partials/fields/nullReplacementValue.md" markdown="true" >}}|

### Examples

```json
{
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
|minLength|Integer|false|The minimum length of the string value.|
|maxLength|Integer|false|The maximum length of the string value.|
|regex|String|false|A [regular expression](https://en.wikipedia.org/wiki/Regular_expression) to validate the input value against e.g. `[a-z]*` would match a value made of only lowercase alphabet characters.|
|trim|Boolean|true|{{< readfile file="/content/partials/fields/trim.md" markdown="true" >}}|
|nullable|Boolean|true|{{< readfile file="/content/partials/fields/nullable.md" markdown="true" >}}|
|nullableValues|Array[String]|false|{{< readfile file="/content/partials/fields/nullableValues.md" markdown="true" >}}|
|nullReplacementValue|String|false|{{< readfile file="/content/partials/fields/nullReplacementValue.md" markdown="true" >}}|

### Examples

```json
{
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

## Struct

A `struct` is a nested field similar to a `map` in most programming languages. It can be used to support complex data types.

### Additional Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|fields|Array|true|A list of [schema](../schema/) fields.|
|nullable|Boolean|true|{{< readfile file="/content/partials/fields/nullable.md" markdown="true" >}}|

### Examples

```json
{
  "name" : "customer_name",
  "description" : "Customer Name",
  "type" : "struct",
  "nullable" : true,
  "metadata": {
    "primaryKey" : true,
    "position": 1
  },
  "fields": [
    {
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
    },
    {
      "name" : "last_name",
      "description" : "Customer Last Name",
      "type" : "string",
      "trim" : true,
      "nullable" : true,
      "nullableValues" : [ "", "null" ],
      "metadata": {
        "primaryKey" : true,
        "position": 1
      }
    }
  ]
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
|trim|Boolean|true|{{< readfile file="/content/partials/fields/trim.md" markdown="true" >}}|
|nullable|Boolean|true|{{< readfile file="/content/partials/fields/nullable.md" markdown="true" >}}|
|nullableValues|Array[String]|false|{{< readfile file="/content/partials/fields/nullableValues.md" markdown="true" >}}|
|nullReplacementValue|String|false|{{< readfile file="/content/partials/fields/nullReplacementValue.md" markdown="true" >}}|

### Examples

```json
{
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
|formatters|Array[String]|true|{{< readfile file="/content/partials/fields/dateFormatters.md" markdown="true" >}}<br><br>Custom formats `ssssssssss` and `sssssssssssss` have been added to support epoch time (i.e. `1527727035`) and epoch millis time (i.e. `1527727035456`) respectively. Both require `timezoneId` of `UTC`.|
|timezoneId|String|true|The timezone of the incoming timestamp using [SimpleDateFormat](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html#timezone) supported timezones. If the `formatter` contains timezone information that will take precendence to this value.<br><br>All timestamps are internally stored in [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time) to allow correctly sequenced events when dealing with events from multiple systems which may run with different internal timezones.|
|time|Map[String, Integer]|false|Use this capability if converting a Date label into a Timestamp for relative comparisons. Required fields are `hour`, `minute`, `second` and `nano` . These values can be agreed with source data suppliers to ensure intra-system data alignment. See below for example.|
|trim|Boolean|true|{{< readfile file="/content/partials/fields/trim.md" markdown="true" >}}|
|nullable|Boolean|true|{{< readfile file="/content/partials/fields/nullable.md" markdown="true" >}}|
|nullableValues|Array[String]|false|{{< readfile file="/content/partials/fields/nullableValues.md" markdown="true" >}}|
|nullReplacementValue|String|false|{{< readfile file="/content/partials/fields/nullReplacementValue.md" markdown="true" >}}|

### Examples

```json
{
  "name" : "create_date",
  "description" : "Customer Creation Date",
  "type" : "timestamp",
  "trim" : true,
  "nullable" : true,
  "nullableValues" : [ "", "null" ],
  "formatters": [
    "dd/MM/uuuu HH:mm:ssZ",
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
