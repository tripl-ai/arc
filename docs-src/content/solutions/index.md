---
title: Common Solutions
weight: 95
type: blog
---

This section describes some job design patterns to deal with common ETL requirements.

## Database Inconsistency

When writing data to targets like databases using the `JDBCLoad` raises a risk of [stale reads](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Isolation_levels) where a client is reading a dataset which is either old or one which is in the process of being updated and so is internally inconsistent.

### Example

- create a new table each run using a `JDBCLoad` stage with a dynamic destination table specified as the `${JOB_RUN_DATE}` environment variable (easily created with GNU [date](https://www.gnu.org/software/coreutils/manual/html_node/Examples-of-date.html) like: `$(date +%Y-%m-%d)`)
- the `JDBCLoad` will only complete successfully once the record count of source and target data have been confirmed to match
- execute a `JDBCExecute` stage to perform a change to a view on the database to point to the new version of the table in a transaction-safe manner
- if the job fails during any of these stages then the users will be unaware and will continue to consume the `customers` view which has the latest successful data

{{< readfile file="/resources/docs_resources/PatternsDatabaseInconsistency" highlight="json" >}}

Where the `update_customer_view.sql` statement is:

```sql
CREATE OR REPLACE VIEW customers AS
SELECT * FROM customers_${JOB_RUN_DATE}
```

Each of the main SQL databases behaves slighly different and has slighty different syntax but most can achieve a repointing of a view to a different table in an atomic operation (as it is a single statement).

Note that this method will require some cleanup activity to be performed or the number of tables will grow with each execution. A second `JDBCExecute` stage could be added to clean up older verions of the underlying `customers_` tables after successful 'rollover' execution.

## Delta Processing

{{<note title="Delta Processing">}}
Databricks have open sourced their Spark Delta Processing framework [DeltaLake](https://delta.io) which provides a much safer (transactional) way to perform updates to a dataset which should be used to prevent stale reads or corruption. See [DeltaLakeExtract](../extract/#deltalakeextract) and [DeltaLakeLoad](../load/#deltalakeload) for implementations.
{{</note>}}

A common pattern is to reduce the amount of computation by processing only new files thereby reducing the amount of processing (and therefore cost) of expensive operations like the [TypingTransform](../transform/#typingtransform).

A simple way to do this is to use the `glob` capabilities of Spark to [extract](../extract) a subset of files and then use a [SQLTransform](../transform/#sqltransform) to merge them with a previous state stored in something like Parquet. It is suggested to have a large date overlap with the previous state dataset to avoid missed data. Be careful with this pattern as it assumes that the previous state is correct/complete and that no input files are late arriving.

### Example

Assuming an input file structure like:

```bash
hdfs://datalake/input/customer/customers_2019-02-01.csv
hdfs://datalake/input/customer/customers_2019-02-02.csv
hdfs://datalake/input/customer/customers_2019-02-03.csv
hdfs://datalake/input/customer/customers_2019-02-04.csv
hdfs://datalake/input/customer/customers_2019-02-05.csv
hdfs://datalake/input/customer/customers_2019-02-06.csv
hdfs://datalake/input/customer/customers_2019-02-07.csv
```

Add an additional environment variable to the `docker run` command which will calculate the delta processing period. By using GNU [date](https://www.gnu.org/software/coreutils/manual/html_node/Examples-of-date.html) the date maths of crossing month/year boundaries is easy and the formatting can be changed to suit your file naming convention.

```bash
-e ETL_CONF_DELTA_PERIOD="$(date --date='3 days ago' +%Y-%m-%d),$(date --date='2 days ago' +%Y-%m-%d),$(date --date='1 days ago' +%Y-%m-%d),$(date +%Y-%m-%d),$(date --date='1 days' +%Y-%m-%d)"
```

Which will expose and environment variable that looks like `ETL_CONF_DELTA_PERIOD=2019-02-04,2019-02-05,2019-02-06,2019-02-07,2019-02-08`.

Alternatively, a [Dynamic Configuration Plugin](../extend/#dynamic-configuration-plugins) like the [arc-deltaperiod-config-plugin](https://github.com/tripl-ai/arc-deltaperiod-config-plugin) can be used to generate a similar list of dates.

This can then be used to read just the files which match the `glob` pattern:

```json
{
  "type": "DelimitedExtract",
  "name": "load customer extract deltas",
  "environments": [
    "production",
    "test"
  ],
  "inputURI": "hdfs://datalake/input/customer/customers_{"${ETL_CONF_DELTA_PERIOD}"}.csv",
  "outputView": "customer_delta_untyped"
},
{
  "type": "TypingTransform",
  "name": "apply data types to only the delta records",
  "environments": [
    "production",
    "test"
  ],
  "inputURI": "hdfs://datalake/metadata/customer.json",
  "inputView": "customer_delta_untyped",
  "outputView": "customer_delta"
},
{
  "type": "ParquetExtract",
  "name": "load customer snapshot",
  "environments": [
    "production",
    "test"
  ],
  "inputURI": "hdfs://datalake/output/customer/customers.parquet",
  "outputView": "customer_snapshot"
},
{
  "type": "SQLTransform",
  "name": "merge the two datasets",
  "environments": [
    "production",
    "test"
  ],
  "inputURI": "hdfs://datalake/sql/select_most_recent_customer.sql",
  "outputView": "customer"
}
```

In this case the files for dates `2019-02-01,2019-02-02,2019-02-03` will not be read as they are not in the `${ETL_CONF_DELTA_PERIOD}` input array.

A SQL `WINDOW` can then be used to find the most recent record:

```sql
-- select only the most recent update record for each 'customer_id'
SELECT *
FROM (
     -- rank the dataset by the 'last_updated' timestamp for each primary keys of the table ('customer_id')
    SELECT
         *
        ,ROW_NUMBER() OVER (PARTITION BY 'customer_id' ORDER BY COALESCE('last_updated', CAST('1970-01-01 00:00:00' AS TIMESTAMP)) DESC) AS row_number
    FROM (
        SELECT *
        FROM customer_snapshot

        UNION ALL

        SELECT *
        FROM customer_delta
    ) customers
) customers
WHERE row_number = 1
```


## Duplicate Keys

To find duplicate keys and stop the job so any issues are not propogated can be done using a `SQLValidate` stage which will fail with a list of invalid `customer_id`s if more than one are found.

### Example

```sql
SELECT
    COUNT(*) = 0
    ,TO_JSON(NAMED_STRUCT(
      'duplicate_customer_count', COUNT(*),
      'duplicate_customer', CAST(COLLECT_LIST(DISTINCT customer_id) AS STRING)
    ))
FROM (
    SELECT
        customer_id
        ,COUNT(customer_id) AS customer_id_count
    FROM customer
    GROUP BY customer_id
) valid
WHERE customer_id_count > 1
```

## Fixed Width Input Formats

It is also quite common to recieve fixed width formats from older systems like IBM Mainframes like:

| data |
|------|
|detail2016-12-1914.23|
|detail2016-12-20-3.98|
|detail2016-12-2118.20|

### Example

- Use a [TextExtract](../extract/#textextract) stage to return dataset of many rows but single column.
- Use a [SQLTransform](../transform/#sqltransform) stage to split the data into columns.

```sql
SELECT
    SUBSTRING(data, 0, 6) AS _type
    ,SUBSTRING(data, 7, 8) AS date
    ,SUBSTRING(data, 17, 4) AS total
FROM fixed_width_demo
```

- Use a `TypingTransform` stage to apply data types to the string columns returned by `SQLTransform`.

## Foreign Key Constraint

Another common data quality check is to check Foreign Key integrity, for example ensuring a customer record exists when loading an accounts dataset.

### Example

#### Customers

|customer_id|customer_name|
|-----------|-------------|
|29728375|Eleazar Stehr|
|69752261|Lisette Roberts|

#### Accounts

|customer_id|account_id|account_name|
|-----------|----------|------------|
|29728375|44205457|Checking Account|
|51805256|25102441|Credit Card Account|
|69752261|80393015|Savings Account|
|69752261|81704186|Credit Card Account|
|44953646|75082852|Personal Loan Account|

This can be done using a `SQLValidate` stage which will fail with a list of invalid accounts if any customer records are missing (be careful of not overloading your logging solution with long messages).

```sql
SELECT
    SUM(invalid_customer_id) = 0
    ,TO_JSON(NAMED_STRUCT(
      'customers', COUNT(DISTINCT customer_id),
      'invalid_account_numbers_count', SUM(invalid_customer_id),
      'invalid_account_numbers', CAST(collect_list(DISTINCT invalid_account_numbers) AS STRING)
    ))
FROM (
    SELECT
        account.account_number
        ,customer.customer_id
        ,CASE
            WHEN customer.customer_id IS NULL THEN account.account_number
            ELSE null
        END AS invalid_account_numbers
        ,CASE
            WHEN customer.customer_id IS NULL THEN 1
            ELSE 0
        END AS invalid_customer_id
    FROM account
    LEFT JOIN customer ON account.customer_id = customer.customer_id
) valid
```

## Header/Trailer Load Assurance

It is common to see formats like where the input dataset contains multiple record types with a trailer for some sort of load assurance/validation which allows processing this sort of data and ensure all records are successful.

| col0 | col1 | col2 | col3 |
|------|------|------|------|
|header|2016-12-21|daily totals|
|detail|2016-12-19|daily total|14.23|
|detail|2016-12-20|daily total|-3.98|
|detail|2016-12-21|daily total|18.20|
|trailer|3|28.45|

### Example

- First use a `DelimitedExtract` stage to load the data into text columns.
- Use two `SQLTransform` stages to split the input dataset into two new `DataFrame`s using SQL `WHERE` statements.

#### detail

```sql
SELECT
    col0 AS _type
    ,col1 AS date
    ,col2 AS description
    ,col3 AS total
FROM raw
WHERE col0 = 'detail'
```

|_type|date|description|total|
|-----|----|-----------|-----|
|detail|2016-12-19|daily total|14.23|
|detail|2016-12-20|daily total|-3.98|
|detail|2016-12-21|daily total|18.20|

#### trailer

```sql
SELECT
    col0 AS _type
    ,col1 AS trailer_records
    ,col2 AS trailer_balance
FROM raw
WHERE col0 = 'trailer'
```

|_type|trailer_records|trailer_balance|
|-----|---------------|---------------|
|trailer|3|28.45|

- Use two `TypingTransform` stages to apply data correct types to the two datasets.
- Use a `SQLValidate` stage to ensure that the count and sum of the `detail` dataset equals that of the `trailer` dataset.

```sql
SELECT
    sum_total = trailer_balance AND records_total = trailer_records
    ,TO_JSON(NAMED_STRUCT(
      'expected_count', trailer_records,
      'actual_count', records_total,
      'expected_balance', trailer_balance,
      'actual_balance', sum_total
    ))
FROM (
    (SELECT COUNT(total) AS records_total, SUM(total) AS sum_total FROM detail) detail
    CROSS JOIN
    (SELECT trailer_records, trailer_balance FROM trailer) trailer
) valid
```

## Machine Learning Model as a Service

To see an example of how to host a simple model as a service (in this case [resnet50](https://www.kaggle.com/keras/resnet50)) see:<br>
https://github.com/tripl-ai/arc/tree/master/src/it/resources/flask_serving

To see how to host a [TensorFlow Serving](https://www.tensorflow.org/serving/) model see:<br>
https://github.com/tripl-ai/arc/tree/master/src/it/resources/tensorflow_serving

To easily scale these services without managed infrastructure you can use [Docker Swarm](https://docs.docker.com/engine/swarm/) which includes a basic load balancer to distribute load across many (`--replicas n`) single-threaded services:

```bash
# start docker services
docker swarm init && \
docker service create --replicas 2 --publish 5000:5000 flask_serving/simple:latest
```

```bash
# to stop docker swarm
docker swarm leave --force
```

## Machine Learning Prediction Thresholds

When used for classification, the [MLTransform](../transform/#mltransform) stage will add a `probability` column which exposes the highest probability score from the Spark ML probability vector which led to the predicted value. This can then be used as a boundary to prevent low probability predictions being sent to other systems if, for example, a change in input data resulted in a major change in predictions.

|id|input|prediction|probability|
|---|-----|----------|-----------|
|4|spark i j k|1.0|0.8403592261212589|
|5|l m n|0.0|0.8378325685476612|
|6|spark hadoop spark|1.0|0.9307336686702373|
|7|apache hadoop|0.0|0.9821575333444208|

### Example

```sql
SELECT
    SUM(low_probability) = 0
    ,TO_JSON(NAMED_STRUCT(
      'probability_below_threshold', SUM(low_probability),
      'threshold', 0.8
    ))
FROM (
    SELECT
        CASE
            WHEN customer_churn.probability < 0.8 THEN 1
            ELSE 0
        END AS low_probability
    FROM customer_churn
) valid
```

The threshold value could be easily passed in as a `sqlParam` parameter and referenced as `${CUSTOMER_CHURN_PROBABILITY_THRESHOLD}` in the SQL code.

## Nested Data

Because the SQL language wasn't really designed with nested data that Spark supports it can be difficult to convert nested data into normal table structures. The [EXPLODE](https://spark.apache.org/docs/latest/api/sql/index.html#explode) and [POSEXPLODE](https://spark.apache.org/docs/latest/api/sql/index.html#posexplode) SQL functions are very useful for this conversion:

### Example

Assuming a nested input structure like a JSON response that has been parsed via `JSONExtract`:

```json
{
  "result": "success",
  "data": [
    {
      "customerId": 1,
      "active": true
    },
    {
      "customerId": 2,
      "active": false
    },
    {
      "customerId": 3,
      "active": true
    }
  ]
}
```

To flatten the `data` array use a SQL subquery and a [POSEXPLODE](https://spark.apache.org/docs/latest/api/sql/index.html#posexplode) to extract the data. `EXPLODE` and `POSEXPLODE` will both produce a field called `col` which can be used from the parent query. `POSEXPLODE` will include an additional field `pos` to indicate the index of the value in the input array (which can is valuable if array order is important for business logic).

```sql
SELECT
  result
  ,pos
  ,col.*
FROM (
  SELECT
    result
    ,POSEXPLODE(data)
  FROM result
) result
```

To produce:

```bash
+-------+---+------+----------+
|result |pos|active|customerId|
+-------+---+------+----------+
|success|0  |true  |1         |
|success|1  |false |2         |
|success|2  |true  |3         |
+-------+---+------+----------+
```

## Testing with Parquet

If you want to manually create test data to compare against a Spark DataFrame a good option is to use the [Apache Arrow](https://arrow.apache.org/) library and the Python API to create a correctly typed [Parquet](https://parquet.apache.org/). This file can then be loaded and compared with the [EqualityValidate](../validate/#equalityvalidate) stage.

### Example

Using the publicly available [Docker](https://www.docker.com/) [Conda](https://conda.io) image:

```bash
docker run -it -v $(pwd)/data:/tmp/data conda/miniconda3 python
```

Then with Python (normally you would install the required libraries into your own Docker image instead of installing dependencies on each run):

```python
# install pyarrow - build your docker image so this is already installed
import subprocess
subprocess.call(['conda', 'install', '-y', '-c', 'conda-forge', 'pyarrow'])

# imports
import decimal
import datetime
import pytz
import pyarrow as pa
import pyarrow.parquet as pq

# create two rows (columnar) of each core data type corresponding with the metadata format
# be careful with null type here as it will be silently converted to a null IntegerType and will not match Spark's NullType
booleanDatum = pa.array([True, False], type=pa.bool_())
dateDatum = pa.array([datetime.date(2016, 12, 18), datetime.date(2016, 12, 19)])
decimalDatum = pa.array([decimal.Decimal('54.321'), decimal.Decimal('12.345')], type=pa.decimal128(38, 18))
doubleDatum = pa.array([42.4242, 21.2121], type=pa.float64())
integerDatum = pa.array([17, 34], type=pa.int32())
longDatum = pa.array([1520828868, 1520828123], type=pa.int64())
stringDatum = pa.array(['test,breakdelimiter', 'breakdelimiter,test'], type=pa.string())
timestampDatum = pa.array([datetime.datetime(2017, 12, 20, 21, 46, 54, 0, tzinfo=pytz.UTC), datetime.datetime(2017, 12, 29, 17, 21, 49, 0, tzinfo=pytz.UTC)])
timeDatum = pa.array(['12:34:56', '23:45:16'], type=pa.string())
nullDatum = pa.array([None, None], type=pa.null())

# create the arrow table
# we are using an arrow table rather than a dataframe to correctly align with spark datatypes
table = pa.Table.from_arrays([booleanDatum, dateDatum, decimalDatum, doubleDatum, integerDatum, longDatum, stringDatum, timestampDatum, timeDatum, nullDatum],
  ['booleanDatum', 'dateDatum', 'decimalDatum', 'doubleDatum', 'integerDatum', 'longDatum', 'stringDatum', 'timestampDatum', 'timeDatum', 'nullDatum'])

# write table to disk
pq.write_table(table, '/tmp/data/example.parquet', flavor='spark')
```

The suggestion then is to use the `environments` key to only execute the [EqualityValidate](../validate/#equalityvalidate) stage whilst in testing mode:

{{< readfile file="/resources/docs_resources/PatternsTestingWithParquet" highlight="json" >}}


