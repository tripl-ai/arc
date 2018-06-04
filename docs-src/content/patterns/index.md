---
title: Patterns
weight: 90
type: blog
---

## Database Inconsistency

When writing data to targets like databases using the `JDBCLoad` raises a risk of 'stale reads' where a client is reading a dataset which is either old or one which is in the process of being updated and so is internally inconsistent. A pattern for preventing this is to:

- create a new table each run using a `JDBCLoad` stage with a dynamic destination table specified as the `${JOB_RUN_DATE}` environment variable
- the `JDBCLoad` will only complete successfully once the record count of source and target data have been confirmed to match
- execute a `JDBCExecute` stage to perform a change to a view on the database to point to the new version of the table in a transaction-safe manner
- if the job fails during any of these stages then the users will be unaware and will continue to consume the `customers` view which has the latest successful data

```json
{
    "type": "JDBCLoad",
    "name": "load active customers to web server database",
    "environments": ["production", "test"],
    "inputView": "ative_customers",            
    "jdbcURL": "jdbc:mysql://localhost/mydb",
    "tableName": "customers_"${JOB_RUN_DATE},
    "numPartitions": 10,
    "isolationLevel": "READ_COMMITTED",
    "batchsize": 10000,
    "params": {
        "user": "mydbuser",
        "password": "mydbpassword",
    }
},
{
    "type": "JDBCExecute",
    "name": "update the current view to point to the latest version of the table",
    "environments": ["production", "test"],
    "inputURI": "hdfs://datalake/sql/update_customer_view.sql",          
    "sqlParams": {
        "JOB_RUN_DATE": ${JOB_RUN_DATE}
    },    
    "params": {
        "user": "mydbuser",
        "password": "mydbpassword",
    }
}
```

Where the `update_customer_view.sql` statement is:

```sql
CREATE OR REPLACE VIEW customers AS 
SELECT * FROM customers_${JOB_RUN_DATE}
```

Each of the main SQL databases behaves slighly different and has slighty different syntax but most can achieve a repointing of a view to a different table in an atomic operation. A second `JDBCExecute` stage could also be added to clean up older verions of the underlying `customers_` tables.

## Duplicate Keys

To find duplicate keys and stop the job so any issues are not propogated can be done using a `SQLValidate` stage which will fail with a list of invalid `customer_id`s if more than one are found:

```sql
SELECT 
    COUNT(*) = 0
    ,CASE   
    TO_JSON(NAMED_STRUCT('duplicate_customer_count', COUNT(*), 'duplicate_customer', CAST(COLLECT_LIST(DISTINCT customer_id) AS STRING)))
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

It is also quite common to recieve fixed width formats from older systems like IBM Mainframes.

| data |
|------|
|detail2016-12-1914.23|
|detail2016-12-20-3.98|
|detail2016-12-2118.20|

- Use a `DelimitedExtract` stage with a delimiter that will not be found in the data like `DefaultHive` to return dataset of many rows but single column.
- Use a `SQLTransform` stage to split the data into columns.

```sql
SELECT 
    SUBSTRING(data, 0, 6) AS _type
    ,SUBSTRING(data, 7, 8) AS date
    ,SUBSTRING(data, 17, 4) AS total
FROM fixed_width_demo
```

- Use a `TypingTransform` stage to apply data types to the string columns returned by `SQLTransform`.

## Foreign Key Constraint

Another common data quality check is to check Foreign Key integrity, for example ensuring a customer record exists when loading an accounts dataset:

### Customers

|customer_id|customer_name|
|-----------|-------------|
|29728375|Eleazar Stehr|
|69752261|Lisette Roberts|

### Accounts

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
    ,TO_JSON(NAMED_STRUCT('customers', COUNT(DISTINCT customer_id), 'invalid_account_numbers_count', SUM(invalid_customer_id), 'invalid_account_numbers', CAST(collect_list(DISTINCT invalid_account_numbers) AS STRING)))
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

It is common to see formats like where the input dataset contains multiple record types with a trailer for some sort of load assurance/validation:

| col0 | col1 | col2 | col3 |
|------|------|------|------|
|header|2016-12-21|daily totals|
|detail|2016-12-19|daily total|14.23|
|detail|2016-12-20|daily total|-3.98|
|detail|2016-12-21|daily total|18.20|
|trailer|3|28.45|


- Use two `SQLTransform` stages to split the input dataset into two new `DataFrame`.

### detail

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

### trailer

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
    ,TO_JSON(NAMED_STRUCT('expected_count', trailer_records, 'actual_count', records_total, 'expected_balance', trailer_balance, 'actual_balance', sum_total))
FROM (
    (SELECT COUNT(total) AS records_total, SUM(total) AS sum_total FROM detail) detail
    CROSS JOIN
    (SELECT trailer_records, trailer_balance FROM trailer) trailer
) valid
```

## Machine Learning Prediction Thresholds

When used for classification the `MLTransform` stage will add a `probability` column which exposes the highest probability score from the Spark ML probability vector which led to the predicted value. This can then be used as a boundary to prevent low probability predictions being sent to other systems if, for example, a change in input data resulted in a major change in predictions. The threshold parameter could be easily passed in as a sqlParam parameter and referenced as `${CUSTOMER_CHURN_PROBABILITY_THRESHOLD}` in the SQL code.

|id|input|prediction|probability|
|---|-----|----------|-----------|
|4|spark i j k|1.0|0.8403592261212589|
|5|l m n|0.0|0.8378325685476612|
|6|spark hadoop spark|1.0|0.9307336686702373|
|7|apache hadoop|0.0|0.9821575333444208|

```sql
SELECT 
    SUM(low_probability) = 0
    ,TO_JSON(NAMED_STRUCT('probability_below_threshold', SUM(low_probability), 'threshold', 0.8))
FROM (
    SELECT 
        CASE 
            WHEN customer_churn.probability < 0.8 IS NULL THEN 1 
            ELSE 0 
        END AS low_probability
    FROM customer_churn
) valid
```

## Testing

If you want to manually create test data to compare against a Spark DataFrame a good option is to use the [Apache Arrow](https://arrow.apache.org/) library and the Python API to create a correctly typed Parquet. This file can then be loaded and compared with the `EqualityValidate` stage.

Using the publicly available [Docker](https://www.docker.com/) [Conda](https://conda.io) image:

```bash
docker run -it -v $(pwd)/data:/tmp/data conda/miniconda3 python
```

Then with Python (of course normally you would install the required libraries into your own Docker image):

```python
# install pyarrow - build your docker image so this is already installed
import subprocess
subprocess.call(['conda', 'install', '-y', '-c', 'conda-forge', 'pyarrow'])

# imports
import datetime
import pytz
import pyarrow as pa
import pyarrow.parquet as pq

# create two rows (columnar) of each core data type corresponding with the metadata format
# be careful with null type here as it will be silently converted to a null IntegerType and will not match Spark's NullType
booleanDatum = pa.array([True, False], type=pa.bool_())
dateDatum = pa.array([datetime.date(2016, 12, 18), datetime.date(2016, 12, 19)])
decimalDatum = pa.array([54.321, 12.345], type=pa.decimal128(38, 18))
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
pq.write_table(table, '/tmp/data/example.parquet')
```

The suggestion then is to use the `environments` key to only execute the difference test whilst in testing mode:

```json
{
  "environments": ["test"],
  "type": "ParquetExtract",
  "name": "Load the known valid dataset",
  "inputURI": "hdfs://datalake/correct.parquet",
  "outputView": "correct",            
  "persist": false,
  "authentication": {
  }
},
{
  "environments": ["test"],
  "type": "EqualityValidate",
  "name": "verify calculated equals manually created dataset",
  "leftView": "calculated",            
  "rightView": "correct",              
}
```


