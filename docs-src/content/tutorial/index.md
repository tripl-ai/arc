---
title: Tutorial
weight: 15
type: blog
---

This tutorial works through a real-world example using the excellent [New York City Taxi dataset](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) which has been used many times (see: [Analyzing 1.1 Billion NYC Taxi and Uber Trips, with a Vengeance](http://toddwschneider.com/posts/analyzing-1-1-billion-nyc-taxi-and-uber-trips-with-a-vengeance/) and [A Billion Taxi Rides in Redshift](http://tech.marksblogg.com/billion-nyc-taxi-rides-redshift.html)) due to its 1 billion+ record count and public data available via the [Registry of Open Data on AWS](https://registry.opendata.aws/nyc-tlc-trip-records-pds/).

It is a great dataset as it has a lot of the attributes of real-world data that need to be considered:

- [Schema Evolution](https://en.wikipedia.org/wiki/Schema_evolution) where fields are added/changed/removed over time or data is normalized as patterns emerge - such as the removal of GPS co-ordinates in the later datasets due to privacy concerns.
- How to reliably apply data typing to an untyped source - in this case a [Comma-Separated Values](https://en.wikipedia.org/wiki/Comma-separated_values) format which does not preserve data type information.
- How to build a repeatable and reproducible process which will scale by adding more compute not human effort - the small example is ~40 million records and the large well over 1 billion records.
- How reusable components can be composed to [extract data](/extract/#delimitedextract) with [data types](/transform/#typingtransform), apply rules to ensure [data quality](/validate/#sqlvalidate), enrich the data by executing [SQL statements](/transform/#sqltransform), apply [machine learning transformations](/transform/#mltransform) and [load the data](/load) to one or more targets.

## Get arc-starter

The easiest way to build an Arc job is by cloning [arc-starter](https://github.com/tripl-ai/arc-starter) which is an interactive development environment based on [Jupyter Notebooks](https://jupyter.org/). This tutorial assumes you have cloned this repository.

![arc-starter](/img/arc-starter.png)

```bash
git clone https://github.com/tripl-ai/arc-starter.git
cd arc-starter
```

To start `arc-juptyer` run:

```bash
./develop.sh
```

This script runs a `docker run` command where the only option that needs to be configured is the `-Xmx4096m` to set the memory available to Spark. This value needs to be less than or equal to the amount of memory allocated to Docker (see [here](https://docs.docker.com/docker-for-mac/#resources) for MacOS).

```bash
docker run \
--name arc-jupyter \
--rm \
--volume $(pwd)/examples:/home/jovyan/examples:Z \
--env JAVA_OPTS="-Xmx4096m" \
--entrypoint='' \
--publish 4040:4040 \
--publish 8888:8888 \
{{% arc_jupyter_docker_image %}} \
jupyter notebook \
--ip=0.0.0.0 \
--no-browser \
--NotebookApp.password='' \
--NotebookApp.token=''
```

## Extracting Data

From the Jupyter main screen select `New` then `Arc` under `notebook`. We will be building the job in this notebook.

The first stage we are going to add is a `DelimitedExtract` stage because the source data is in Comma-Separated Values format delimited by '`,`'. This stage will instruct Arc to extract the data in all `.csv` files from the `inputURI` path and register as the internal view `green_tripdata0_raw` so the data can be accessed in subsequent job stages.

```json
{
  "type": "DelimitedExtract",
  "name": "extract data from green_tripdata schema 0",
  "environments": ["production", "test"],
  "inputURI": "s3a://nyc-tlc/trip*data/green_tripdata_2013-08.csv",
  "outputView": "green_tripdata0_raw",
  "delimiter": "Comma",
  "quote" : "DoubleQuote",
  "header": true,
  "persist": true
}
```

By executing this stage (`SHIFT-ENTER`) you should be able to see a result set.

|VendorID|lpep_pickup_datetime|Lpep_dropoff_datetime|Store_and_fwd_flag|RateCodeID|Pickup_longitude|Pickup_latitude|Dropoff_longitude|Dropoff_latitude|Passenger_count|Trip_distance|Fare_amount|Extra|MTA_tax|Tip_amount|Tolls_amount|Ehail_fee|Total_amount|Payment_type|Trip_type|_filename|_index|
|--------|--------------------|---------------------|------------------|----------|----------------|---------------|-----------------|----------------|---------------|-------------|-----------|-----|-------|----------|------------|---------|------------|------------|---------|---------|---------|
|2|2013-09-01 00:02:00|2013-09-01 00:54:51|N|1|-73.952407836914062|40.810726165771484|-73.983940124511719|40.676284790039063|5|14.35|50.5|0.5|0.5|10.3|0|null|61.8|1|null|s3a://nyc-tlc/trip%20data/green_tripdata_2013-08.csv|1
|2|2013-09-01 00:02:34|2013-09-01 00:20:59|N|1|-73.963020324707031|40.711833953857422|-73.966644287109375|40.681690216064453|1|3.24|15|0.5|0.5|0|0|null|16|2|null|s3a://nyc-tlc/trip%20data/green_tripdata_2013-08.csv|2

If you scroll to the very right of the result set you should be able to see two additional columns which is added by Arc to help trace data lineage to assist debugging.

- `_filename`: which records the input file source for all file based imports.
- `_index`: which records the input file row number for all file based imports. As Spark is a distributed system calculating a true row level `_index` is computationally expensive. To reduce the load the import option `"contiguousIndex": false` option can be provided to produce a monotonically increasing identifier from which `_index` can be derived later if required.

The other thing to note is the use of `"persist": true` which instructs Arc to store the the dataset read from the external Amazon s3 bucket into memory. This means that any subsequent stage that references `green_tripdata0_raw` does not need to re-download the file.

## Typing Data

{{< note title="Data Typing" >}}
Data Typing is the process of converting a `text` (or `string`) value to a specific representation such as a `Timestamp` or `Decimal`.

For example, trying to calculate the day of the week of the text `2016-04-09` would require very complex logic however if we first convert to a `Date` type we know:

- the date is actually a valid date (where `2016-02-30` would fail)
- we can use a function like `day_of_week(date)` to get the day of the week which has been tested to be correct
{{</note>}}

To make this data produced above more useful for analysis (for example doing aggregation by time period or summing dollars) we need to **safely** apply data typing.

Add a new stage to apply a `TypingTransformation` to the data extracted in the first stage named `green_tripdata0_raw`. `TypingTransform` parses tabular text data to and, in this case, will produce an output dataset called `green_tripdata0`. To do this we have to tell Arc how to parse the `string` data back into their original data types (like `timestamp` or `integer`). To do this transformation we need some way to pass in the rules of how to parse the data and that is described in the `schema` file passed in using the `schemaURI`.

```json
{
  "type": "TypingTransform",
  "name": "apply green_tripdata schema 0 data types",
  "environments": ["production", "test"],
  "schemaURI": "/home/jovyan/examples/tutorial/0/green_tripdata0.json",
  "inputView": "green_tripdata0_raw",
  "outputView": "green_tripdata0"
}
```

## Specifying Data Typing Rules

The [schema](/schema/) provides the rules needed to parse an untyped (`string`) dataset into a typed dataset. Where a traditional database will fail when a data conversion fails (for example `CAST('abc' AS INT)` would fail) Spark defaults to returning `NULL` which makes safely and precisely parsing data using only Spark SQL very difficult.

Here is the top of of the `/home/jovyan/examples/tutorial/0/green_tripdata0.json` file which provides and example of how the rules defining how to convert `string` values back into their correct data types are represented. This file was manually created based on the [official data dictionary](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) - but frequently you will find determining this information is extremely difficult.

```json
[
  {
    "name": "vendor_id",
    "description": "A code indicating the TPEP provider that provided the record.",
    "trim": true,
    "nullable": true,
    "type": "integer",
    "nullableValues": [
      "",
      "null"
    ]
  },
  {
    "name": "lpep_pickup_datetime",
    "description": "The date and time when the meter was engaged.",
    "trim": true,
    "nullable": true,
    "type": "timestamp",
    "formatters": [
      "uuuu-MM-dd HH:mm:ss"
    ],
    "timezoneId": "America/New_York",
    "nullableValues": [
      "",
      "null"
    ]
  },
  ...
```

Picking one of the more interesting fields, `lpep_pickup_datetime`, we can highlight a few details:

- this is a [timestamp](/schema/#timestamp) field which means it must be generated from a valid combination of date and time.
- the `formatters` key specifies an `array` rather than a simple `string`. This is because real world data often has multiple date/datetime formats used in a single column. By allowing an `array` Arc will try to apply each of the formats specified in sequence and only fail if *none* of the formatters can be successfully applied.
- a mandatory `timezoneId` must be specified. This is required as the only way to reliably work with dates and times across systems is to know when they happened relative to [Coordinated Universal Time](https://en.wikipedia.org/wiki/Coordinated_Universal_Time) (UTC) so they can be placed as point on a universally continuous timeline. Additionally `timezoneId` is specified at a column level meaning that it is possible to have multiple timezones for different columns in the same dataset.
- the `nullableValues` key also specifies an `array` which allows you to specify multiple values which will be converted to a true `null` when loading. If these values are present and the `nullable` key is set to `true` then the job will fail with a clear error message.
- the `description` field is saved with the data some formats like when using [ORCLoad](/load/#orcload), [ParquetLoad](/load/#parquetload) or [DeltaLakeLoad](/load/#deltalakeload) into the underlying metadata and will be restored automatically if those files are re-injested by Arc.

{{< note title="Schema Order" >}}
This format does not use input field names and will only try to convert data by its column index - meaning that the order of the fields in the schema file must match the input dataset.
{{</note>}}

So what happens if this conversion fails?

## Data Typing Validation

Similar to the `DelimitedExtract`, `TypingTransformation` adds an addition field to each row called `_errors` which holds an `array` of data conversion failures - if any exist - so that all the errors in the entire dataset can be determined before deciding how to respond. If a data conversion issue exists then the field `name` and a human readable message will be pushed into that array and the value will be set to `null` for that field:

For example:

```bash
+-------------------+-------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|startTime          |endTime            |_errors                                                                                                                                                                                                                                                             |
+-------------------+-------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|2018-09-26 17:17:43|2018-09-27 17:17:43|[]                                                                                                                                                                                                                                                                  |
|2018-09-25 18:25:51|2018-09-26 18:25:51|[]                                                                                                                                                                                                                                                                  |
|null               |2018-03-01 12:16:40|[[startTime, Unable to convert '2018-02-30 01:16:40' to timestamp using formatters ['uuuu-MM-dd HH:mm:ss'] and timezone 'UTC']]                                                                                                                                     |
|null               |null               |[[startTime, Unable to convert '28 February 2018 01:16:40' to timestamp using formatters ['uuuu-MM-dd HH:mm:ss'] and timezone 'UTC'], [endTime, Unable to convert '2018-03-2018 01:16:40' to timestamp using formatters ['uuuu-MM-dd HH:mm:ss'] and timezone 'UTC']]|
+-------------------+-------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

If you have specified that the field cannot be `null` via `"nullable": false` then the job will fail at this point with an appropriate error message as logically as it cannot continue.

If the job has been configured like above with all fields `"nullable": true` then the `TypingTransform` stage will complete but we will not be actually enforcing that no errors have occured. To add the ability to stop the job based on whether errors occured we can add a `SQLValidate` stage:

```sql
%sqlvalidate name="ensure no errors exist after data typing" environments=production,test
SELECT
  SUM(error) = 0 AS valid
  ,TO_JSON(
      NAMED_STRUCT(
        'count', COUNT(error),
        'errors', SUM(error)
      )
  ) AS message
FROM (
  SELECT
    CASE
      WHEN SIZE(_errors) > 0 THEN 1
      ELSE 0
    END AS error
  FROM green_tripdata0
) input_table
```

The summary of what happens in this SQL statement is:

- sum up the number of rows in the `green_tripdata0` dataset where the `SIZE` of the `_errors` array for each row is greater than 0.  If the errors array is not empty (`SIZE(_errors) > 0`) then there must have been at least one error on that row.
- check that that sum of errors all errors for all rows `SUM(error)` equals `0` so that the first field will return `true` if `SUM(error) = 0` or `false` if `SUM(error) != 0`
- as doing a count is visiting all rows anyway we can emit statistics so the output will be a `json` object that will be added to the logs. In this case we are logging a row count (`COUNT(error)`) and a count of rows with at least 1 error (`SUM(error)`) which will return something like `{"count":7623,"errors":0}`.

{{< note title="Data Caching" >}}
A `TypingTransformation` is a big and computationally expensive operation so if you are going to do multiple operations against that dataset (as we are) set the `"persist": true` option so that Spark will cache the dataset after applying the types.
{{</note>}}

## Saving Data

The final step is to do something with the data. This could be any of the [Load](/load/) stages but for our use case we will do a [DeltaLakeLoad](/load/#deltalakeload). [DeltaLake](https://delta.io) is a great because:

- it 'versions' data allowing users to 'time travel' back and forward through different datasets efficiently.
- it perfoms atomic commits meaning that when used with storage like Amazon S3 it will not leave partial/corrupt datasets if Spark shuts down unexpectedly.
- it uses [Apache Parquet](https://parquet.apache.org/) which is a compressed columnar data format meaning that when it is read by subsequent Spark jobs you can only read the columns that are required vastly reducing the amount of data moved across a network and that has to be processed.
- it retains full data types and metadata so that you don't have to keep converting text to correctly typed data before use.
- it supports data partitioning and pushdown which can further reduce the amount of data required to be read from expensive sources like Amazon S3.

Here is the stage we will add which writes the `green_tripdata0` dataset to a `DeltaLake` dataset on disk. It will also be partitioned by `vendor_id` so that if you were doing analysis on only one of the vendors then Spark could easily read only that data and ignore the other vendors. Here we are explicitly naming the output `.delta` to help future users understand its format.

```json
{
  "type": "DeltaLakeLoad",
  "name": "write out green_tripdata0 dataset",
  "environments": ["production", "test"],
  "inputView": "green_tripdata0",
  "outputURI": "/home/jovyan/examples/tutorial/0/output/green_tripdata0.delta",
  "saveMode": "Overwrite",
  "partitionBy": [
    "vendor_id"
  ]
}
```

## Execute It

At this stage we have a job which will extract data, apply data types to one `.csv` file and execute a `SQLValidate` stage to ensure that the data could be converted successfully then write the data out for future use. The Arc framework is packaged as a [Docker](https://www.docker.com/) image so that you can run the same job on your local machine or a massive compute cluster without having to think about how to deploy dependencies. The default Docker image contains the dependencies files for connecting to most `JDBC` databases and cloud services.

To run the job that is included with the `arc-starter` repository (this file is also saved as `run.sh`):

```bash
docker run \
--rm \
--volume $(pwd)/examples:/home/jovyan/examples:Z \
--env "ETL_CONF_ENV=production" \
--entrypoint='' \
--publish 4040:4040 \
{{% docker_image %}} \
bin/spark-submit \
--master local[*] \
--driver-memory 4g \
--driver-java-options "-XX:+UseG1GC -XX:-UseGCOverheadLimit -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap" \
--conf spark.authenticate=true \
--conf spark.authenticate.secret=$(openssl rand -hex 64) \
--conf spark.io.encryption.enabled=true \
--conf spark.network.crypto.enabled=true \
--class ai.tripl.arc.ARC \
/opt/spark/jars/arc.jar \
--etl.config.uri=file:///home/jovyan/examples/tutorial/0/nyctaxi.ipynb
```

As the job runs you will see `json` formatted logs generated and printed to screen. These can easily be sent to a [log management](https://en.wikipedia.org/wiki/Log_management) solution for log aggregation/analysis/alerts. The important thing is that our job ran and we can see our message `{"count":7623,"errors":0}` formatted as numbers so that it can be easily addressed (`event.message.count`) and compared day-by-day for monitoring.

```json
{
  "event": "exit",
  "status": "success",
  "success": true,
  "duration": 56107,
  "level": "INFO",
  "thread_name": "main",
  "class": "ai.tripl.arc.ARC$",
  "logger_name": "local-1574152663175",
  "timestamp": "2019-11-16 08:38:37.533+0000",
  "environment": "production",
  "streaming": "false",
  "applicationId": "local-1574152663175",
  "ignoreEnvironments": "false"
}
```

A runnable snapshot of this job is available: `examples/tutorial/0/nyctaxi.ipynb`.

Congratulations you have run your first Arc job!

## Viewing the Metadata

Arc allows users to define and store metadata (that is data which describes columns) attached the dataset. This data can be simple things like the `description` field shown below or [more complex metadata](/schema). By storing the metadata with the actual dataset you can safely write out the data using enriched formats like [ParquetLoad](/load/#parquetload) or [DeltaLakeLoad](/load/#deltalakeload) and when those files are read in the future they will have that metadata still attached and in sync.

Numerous stages have been added to explicitly operate against the metadata:

- [MetadataExtract](/extract/#metadataextract) which creates an Arc `metadata` dataframe from a view.
- [MetadataTransform](/transform/#metadatatransform) which allows you to attach/override the metadata attached to a view.
- [MetadataFilterTransform](/transform/#metadatafiltertransform) which allows columns from an input view to be automatically filtered based on their metadata.
- [MetadataValidate](/validate/#metadatavalidate) which allows runtime rules to be aplied against a view's metadata.

Running:

```sql
%metadata
green_tripdata0
```

Will produce an output like:

![arc-starter](/img/metadata.png)

## Environment Variables

{{< note title="JSON vs HOCON" >}}
The config file, whilst looking very similar to a `json` file is actually a [Human-Optimized Config Object Notation](https://en.wikipedia.org/wiki/HOCON) (HOCON) file. This file format is a superset of `json` allowing some very useful extensions like [Environment Variable](https://en.wikipedia.org/wiki/Environment_variable) substitution and string interpolation. We primarily use it for Environment Variable injection but all its capabilities described [here](https://github.com/lightbend/config) can be utilised.
{{</note>}}

For testing and automated deployment it is useful to be able to dynamically change input file locations when we deploy the job across different environments. So, for example, a local `/home/jovyan/tutorial` path could be used for `test` vs a remote `https://raw.githubusercontent.com/tripl-ai/arc-starter/master/tutorial` path when running in `production` mode.

To do this [Environment Variables](https://en.wikipedia.org/wiki/Environment_variable) can be used or the `%env` magic can be used in Jupyter notebooks:

```scala
%env
ETL_CONF_DATA_URL=s3a://nyc-tlc/trip*data
ETL_CONF_JOB_URL=/home/jovyan/examples/tutorial/1
```

The variables can then be used like:

```json
{
  "type": "DelimitedExtract",
  "name": "extract data from green_tripdata schema 0",
  "environments": ["production", "test"],
  "inputURI": ${ETL_CONF_DATA_URL}"/green_tripdata_2013-08.csv*",
  "outputView": "green_tripdata0_raw",
  "delimiter": "Comma",
  "quote": "DoubleQuote",
  "header": true,
  "persist": true
}
```

When executing the job these environment variables can be set like:

```bash
docker run \
--rm \
-v $(pwd)/examples:/home/jovyan/examples:Z \
-e "ETL_CONF_ENV=production" \
-e "ETL_CONF_DATA_URL=s3a://nyc-tlc/trip*data" \
...
```

To speed up the rest of the tutorial the top 50000 rows of each dataset have been embedded in this repository:

```scala
%env
ETL_CONF_DATA_URL=/home/jovyan/examples/tutorial/data/nyc-tlc/trip*data
ETL_CONF_JOB_URL=/home/jovyan/examples/tutorial/1
```

Spark is also able to automatically detect and decompress files based on their extension. Note the `*` after `green_tripdata_2013-08.csv*` which will allow either `.csv` which is on the remote `s3a://nyc-tlc/trip*data/green_tripdata_2013-08.csv` or `/home/jovyan/examples/tutorial/data/nyc-tlc/trip*data/green_tripdata_2013-08.csv.gz` which is local.

```json
{
  "type": "DelimitedExtract",
  "name": "extract data from green_tripdata schema 0",
  "environments": ["production", "test"],
  "inputURI": ${ETL_CONF_DATA_URL}"/green_tripdata_2013-08.csv*",
  "outputView": "green_tripdata0_raw",
  "delimiter": "Comma",
  "quote": "DoubleQuote",
  "header": true,
  "persist": true
}
```

To execute:

```bash
docker run \
--rm \
-v $(pwd)/examples:/home/jovyan/examples:Z \
-e "ETL_CONF_ENV=production" \
-e "ETL_CONF_DATA_URL=/home/jovyan/examples/tutorial/data/nyc-tlc/trip*data" \
...
```

This method allows use of smaller dataset while developing then easily changing out data sources when trying to run in production.

## Add more data

To continue with the `green_tripdata` dataset example we can now add the other two schema versions. This will show the general pattern for adding additional data and dealing with [Schema Evolution](https://en.wikipedia.org/wiki/Schema_evolution). You can see here that adding more data is just appending additional stages following the same pattern:

### green_tripdata0

```json
{
  "type": "DelimitedExtract",
  "name": "extract data from green_tripdata schema 0",
  "environments": ["production", "test"],
  "inputURI": ${ETL_CONF_DATA_URL}"/green_tripdata_2013-08.csv*",
  "outputView": "green_tripdata0_raw",
  "delimiter": "Comma",
  "quote": "DoubleQuote",
  "header": true,
  "persist": true
}
```

```json
{
  "type": "TypingTransform",
  "name": "apply green_tripdata schema 0 data types",
  "environments": ["production", "test"],
  "schemaURI": ${ETL_CONF_JOB_URL}"/green_tripdata0.json",
  "inputView": "green_tripdata0_raw",
  "outputView": "green_tripdata0",
  "persist": true
}
```

```sql
%sqlvalidate name="ensure no errors exist after data typing" environments=production,test
SELECT
  SUM(error) = 0 AS valid
  ,TO_JSON(
      NAMED_STRUCT(
        'count', COUNT(error),
        'errors', SUM(error)
      )
  ) AS message
FROM (
  SELECT
    CASE
      WHEN SIZE(_errors) > 0 THEN 1
      ELSE 0
    END AS error
  FROM green_tripdata0
) input_table
```

### green_tripdata1

```json
{
  "type": "DelimitedExtract",
  "name": "extract data from green_tripdata schema 1",
  "environments": ["production", "test"],
  "inputURI": ${ETL_CONF_DATA_URL}"/green_tripdata_2015-01.csv*",
  "outputView": "green_tripdata1_raw",
  "delimiter": "Comma",
  "quote": "DoubleQuote",
  "header": true,
  "persist": true
}
```

```json
{
  "type": "TypingTransform",
  "name": "apply green_tripdata schema 1 data types",
  "environments": ["production", "test"],
  "schemaURI": ${ETL_CONF_JOB_URL}"/green_tripdata1.json",
  "inputView": "green_tripdata1_raw",
  "outputView": "green_tripdata1",
  "persist": true
}
```

```sql
%sqlvalidate name="ensure no errors exist after data typing" environments=production,test
SELECT
  SUM(error) = 0 AS valid
  ,TO_JSON(
      NAMED_STRUCT(
        'count', COUNT(error),
        'errors', SUM(error)
      )
  ) AS message
FROM (
  SELECT
    CASE
      WHEN SIZE(_errors) > 0 THEN 1
      ELSE 0
    END AS error
  FROM green_tripdata1
) input_table
```

### green_tripdata2

```json
{
  "type": "DelimitedExtract",
  "name": "extract data from green_tripdata schema 2",
  "environments": ["production", "test"],
  "inputURI": ${ETL_CONF_DATA_URL}"/green_tripdata_2016-07.csv*",
  "outputView": "green_tripdata2_raw",
  "delimiter": "Comma",
  "quote": "DoubleQuote",
  "header": true,
  "persist": true
}
```

```json
{
  "type": "TypingTransform",
  "name": "apply green_tripdata schema 2 data types",
  "environments": ["production", "test"],
  "schemaURI": ${ETL_CONF_JOB_URL}"/green_tripdata2.json",
  "inputView": "green_tripdata2_raw",
  "outputView": "green_tripdata2",
  "persist": true
}
```

```sql
%sqlvalidate name="ensure no errors exist after data typing" environments=production,test
SELECT
  SUM(error) = 0 AS valid
  ,TO_JSON(
      NAMED_STRUCT(
        'count', COUNT(error),
        'errors', SUM(error)
      )
  ) AS message
FROM (
  SELECT
    CASE
      WHEN SIZE(_errors) > 0 THEN 1
      ELSE 0
    END AS error
  FROM green_tripdata2
) input_table
```

Now we have created three correctly typed and validated datasets in memory (`green_tripdata0`, `green_tripdata1` and `green_tripdata2`). How are they merged?

## Merging Data

The real complexity with schema evolution comes defining clear rules with how to deal with fields which are added and removed over time. In the case of `green_tripdata` the main change over time is the change from giving specific pickup and dropoff co-ordinates (`pickup_longitude`, `pickup_latitude`, `dropoff_longitude`, `dropoff_latitude`) in the early datasets (`green_tripdata0` and `green_tripdata1`) to only providing more generalised (and much more private) `pickup_location_id` and `dropoff_location_id` geographic regions in `green_tripdata2`.

The recommended way to deal with this is to use [SQL](https://en.wikipedia.org/wiki/SQL) to manually define the rules for each dataset before unioning the data together via `UNION ALL`. Here the query explicitly deals with the lack of `pickup_longitude` data in `green_tripdata2` by setting a `NULL` value but retaining the column so the `UNION` still works. The alternative, depending on use of this data, would be to remove the `pickup_longitude` column from the later datasets.

```sql
%sql name="combine green_tripdata_*" environments=production,test outputView=trips
-- first schema 2013-08 to 2014-12
SELECT
  vendor_id
  ,lpep_pickup_datetime AS pickup_datetime
  ,lpep_dropoff_datetime AS dropoff_datetime
  ,store_and_fwd_flag
  ,rate_code_id
  ,pickup_longitude
  ,pickup_latitude
  ,dropoff_longitude
  ,dropoff_latitude
  ,passenger_count
  ,trip_distance
  ,fare_amount
  ,extra
  ,mta_tax
  ,tip_amount
  ,tolls_amount
  ,ehail_fee
  ,NULL AS improvement_surcharge
  ,total_amount
  ,payment_type AS payment_type_id
  ,NULL AS trip_type_id
  ,NULL AS pickup_location_id
  ,NULL AS dropoff_location_id
FROM green_tripdata0

UNION ALL

-- second schema 2015-01 to 2016-06
SELECT
  vendor_id
  ,lpep_pickup_datetime AS pickup_datetime
  ,lpep_dropoff_datetime AS dropoff_datetime
  ,store_and_fwd_flag
  ,rate_code_id
  ,pickup_longitude
  ,pickup_latitude
  ,dropoff_longitude
  ,dropoff_latitude
  ,passenger_count
  ,trip_distance
  ,fare_amount
  ,extra
  ,mta_tax
  ,tip_amount
  ,tolls_amount
  ,ehail_fee
  ,improvement_surcharge
  ,total_amount
  ,payment_type AS payment_type_id
  ,NULL AS trip_type_id
  ,NULL AS pickup_location_id
  ,NULL AS dropoff_location_id
FROM green_tripdata1

UNION ALL

-- third schema 2016-07 +
SELECT
  vendor_id
  ,lpep_pickup_datetime AS pickup_datetime
  ,lpep_dropoff_datetime AS dropoff_datetime
  ,store_and_fwd_flag
  ,rate_code_id
  ,NULL AS pickup_longitude
  ,NULL AS pickup_latitude
  ,NULL AS dropoff_longitude
  ,NULL AS dropoff_latitude
  ,passenger_count
  ,trip_distance
  ,fare_amount
  ,extra
  ,mta_tax
  ,tip_amount
  ,tolls_amount
  ,ehail_fee
  ,improvement_surcharge
  ,total_amount
  ,payment_type AS payment_type_id
  ,NULL AS trip_type_id
  ,pickup_location_id
  ,dropoff_location_id
FROM green_tripdata2
```

A runnable snapshot of this job is available:  `examples/tutorial/1/nyctaxi.ipynb`.

## Don't Repeat Yourself

A common principle in software development is [Don't Repeat Yourself](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) (aka DRY) which in a Data Engineering context loosely means that logic should not repeated in multiple places to avoid inconsistencies.

In the job above we have defined the same logic to `%sqlvalidate` that there are no errors after applying datatypes multiple times but have coded it each time for each version of `green_tripdata`. If the business rules changed in that we were prepared to accept a five percent data typing error rate we would have to change the logic in multiple places which could easily be incorrectly applied or missed. A safer way to implement this is to externalise the logic and refer to it when needed.

First make a SQL statement that accepts the parameter `${inputView}` to pass in the dataset to verify. Save this file in `examples/tutorial/1/sqlvalidate_errors.sql`

```sql
SELECT
  SUM(error) = 0 AS valid
  ,TO_JSON(
      NAMED_STRUCT(
        'count', COUNT(error),
        'errors', SUM(error)
      )
  ) AS message
FROM (
  SELECT
    CASE
      WHEN SIZE(_errors) > 0 THEN 1
      ELSE 0
    END AS error
  FROM ${inputView}
) input_table
```

Replace the three `%sqlvalidate` statements with `SQLValidate` stages which reference that file.

```json
{
  "type": "SQLValidate",
  "name": "ensure no errors exist after data typing",
  "environments": ["production", "test"],
  "inputURI": ${ETL_CONF_JOB_URL}"/sqlvalidate_errors.sql",
  "sqlParams": {
    "inputView": "green_tripdata0"
  }
}
```

In this case before the SQL statement is executed the named parameter `${inputView}` in the SQL statement will be replaced with `green_tripdata0` so it will validate the `green_tripdata0` dataset. The benefit of this is that the same SQL statement can be used for any dataset after the `TypingTransformation` stage to ensure there are no data typing errors and all we have to do is specify a different `inputView` substitution value. Now if the business rule changed only the `sqlvalidate_errors.sql` would need to be updated.

This method also works for the `%sql` stage used to merge the data where we can define a [SQLTransform](/transform/#sqltransform) stage to execute an external SQL statement.

```json
{
  "type": "SQLTransform",
  "name": "combine green_tripdata_*",
  "environments": ["production", "test"],
  "inputURI": ${ETL_CONF_JOB_URL}"/trips.sql",
  "outputView": "trips",
  "persist": false,
  "sqlParams": {}
}
```

## Glob Pattern Matching

Arc allows for pattern matching of file names including the standard wildcard (`green_tripdata_*.csv`) or more advanced [Glob](https://en.wikipedia.org/wiki/Glob_%28programming%29) matching. Glob can be used to select subsets of data in a directory but generally we recommend using directories and wildcards such as `green_tripdata/0/*.csv` to simplify the separation of dataset schema versions.

**Warning**: consider the size of the data before setting these options as they can be very large.

#### Small Dataset

| dataset | schema | glob pattern | size |
|---------|--------|-----------------------|------|
|green_tripdata|0|`green_tripdata_2013-08.csv`|1.1MB|
|green_tripdata|1|`green_tripdata_2015-01.csv`|232.0MB|
|green_tripdata|2|`green_tripdata_2016-07.csv`|116.6MB|
|yellow_tripdata|0|`yellow_tripdata_2009-01.csv`|2.4GB|
|yellow_tripdata|1|`yellow_tripdata_2015-01.csv`|1.8GB|
|yellow_tripdata|2|`yellow_tripdata_2016-07.csv`|884.7MB|

#### Full Dataset

| dataset | schema | glob pattern | size |
|---------|--------|-----------------------|------|
|green_tripdata|0|`green_tripdata_{2013-*,2014-*}.csv`|2.5GB|
|green_tripdata|1|`green_tripdata_{2015-*,2016-{01,02,03,04,05,06}}.csv`|4.2GB|
|green_tripdata|2|`green_tripdata_{2016-{07,08,09,10,11,12},2017-*}.csv`|1.6GB|
|yellow_tripdata|0|`yellow_tripdata_{2009-*,2010-*,2011-*,2012-*,2013-*,2014-*}.csv`|172.5GB|
|yellow_tripdata|1|`yellow_tripdata_{2015-*,2016-{01,02,03,04,05,06}}.csv`|31.4GB|
|yellow_tripdata|2|`yellow_tripdata_{2016-{07,08,09,10,11,12}}.csv`|5.2GB|

## Add the rest of the tables

Use the patterns above to add the `yellow_tripdata` datasets to the Arc job.

- add the file loading for the `yellow_tripdata`. There should be 3 stages for each schema load (`DelimitedExtract`, `TypingTransform`, `SQLValidate`) and a total of 6 schema versions (3 `green_tripdata` and 3 `yellow_tripdata`) for a total of 18 stages just to read and safely type the data.
- modify the `trips` [SQLTransform](/transform/#sqltransform) to include the new datasets (and handle the merge rules).

## Data Quality Reporting

A secondary use for the `SQLValidate` stage is find data which does not comply with your user-defined data quality rules.

For example, when thinking about how taxis operate we intuitively know that:

- a taxi that has moved a distance should have charged greater than $0.
- a taxi that has charged greater than $0 should have moved a distance.
- a taxi that has moved a distance should have at least 1 passenger.

That means we can code rules to find these scenarios for reporting by setting the first value to `TRUE` (so that the job will always continue past this stage):

```sql
SELECT
  TRUE AS valid
  ,TO_JSON(
      NAMED_STRUCT(
        'count', COUNT(*),
        'distance_without_charge', SUM(distance_without_charge),
        'charge_without_distance', SUM(charge_without_distance),
        'distance_without_passenger', SUM(distance_without_passenger)
      )
  ) AS message
FROM (
  SELECT
    CASE
      WHEN trip_distance > 0 AND fare_amount = 0 THEN 1
      ELSE 0
    END AS distance_without_charge,
    CASE
      WHEN trip_distance = 0 AND fare_amount > 0 THEN 1
      ELSE 0
    END AS charge_without_distance
    ,CASE
      WHEN trip_distance > 0 AND passenger_count = 0 THEN 1
      ELSE 0
    END AS distance_without_passenger
  FROM ${inputView}
) input_table
```

When run against the `green_tripdata0` dataset this produces a series of numbers which can easily be used to produce graphs of errors over time via a dashboard in your log aggregation tool:

```json
{
  "count": 7623,
  "distance_without_charge": 2,
  "charge_without_distance": 1439,
  "distance_without_passenger": 1
}
```

A runnable snapshot of this job is available:  `examples/tutorial/2/nyctaxi.ipynb`.

## Dealing with Empty Datasets

Sometimes you want to deploy a change to production before the files arrive so that the job will automatically include that new data once it starts arriving. Arc supports this by allowing you to specify a schema for an empty dataset so that if no data arrives in a target directory `inputURI` an empty dataset with the correct columns and column types is created so that all subsequent stages that depend on that dataset execute without failure.

For example, imagine an example where we know a new a new `yellow_tripdata` schema (version `3`) starts in the year `2030` where it has been decided that providing the `tpep_pickup_datetime` and `tpep_dropoff_datetime` fields is no longer acceptable so they have been removed:

Add a `schemaURI` key which points to the same metadata file used by the subsequent `TypingTransform` stage (`yellow_tripdata3.json` has been modified to remove the two fields). When this is executed, because there is no input data, an empty (0 rows) dataset with the columns and types specified in `yellow_tripdata3.json` will be created:

```json
{
  "type": "DelimitedExtract",
  "name": "extract data from green_tripdata schema 3",
  "environments": ["production", "test"],
  "inputURI": ${ETL_CONF_DATA_URL}"/yellow_tripdata_2030*.csv",
  "schemaURI": ${ETL_CONF_JOB_URL}"/yellow_tripdata3.json",
  "outputView": "yellow_tripdata3_raw",
  "delimiter": "Comma",
  "quote": "DoubleQuote",
  "header": true,
  "persist": true
}
```

Also because we are testing that file for data quality using `SQLValidate` we need to change the SQL statement to be able to deal with empty datasets by adding a `COALESCE` to the first return value:

```sql
SELECT
  COALESCE(SUM(error) = 0, TRUE) AS valid
  ,TO_JSON(
      NAMED_STRUCT(
        'count', COUNT(error),
        'errors', COALESCE(SUM(error),0)
      )
  ) AS message
FROM (
  SELECT
    CASE
      WHEN SIZE(_errors) > 0 THEN 1
      ELSE 0
    END AS error
  FROM ${inputView}
) input_table
```

The `trips.sql` has been modified to `UNION ALL` the new `yellow_tripdata3` dataset which is going to have 0 rows for another ~10 years. This means that this job can now be safely deployed to production and will contain the new data assuming that when the data starts arriving in `2030` it complies with our deployed `yellow_tripdata3.json` schema.

A runnable snapshot of this job is available:  `examples/tutorial/3/nyctaxi.ipynb`.

## Reference Data

As the business problem is better understood it is common to see [normalization of data](https://en.wikipedia.org/wiki/Database_normalization). For example, in the `yellow_tripdata0` in the early datasets `payment_type` was a `string` field which led to values which were variations of the same intent like `cash` and `CASH`. In the later datasets the `payment_type` has been normailized into a dataset which maps the `'cash'` type to the value `2`. To normalise this data we first need to load a lookup table which is going to define the rules on how to map `payment_type` (`cash`) to `payment_type_id` (`2`). One of the benefits of using a `json` formatted file for this type of reference data is it can easily be used with `git` to track changes over time.

```json
{
  "type": "JSONExtract",
  "name": "load cab_type_id reference table",
  "environments": ["production", "test"],
  "inputURI": ${ETL_CONF_JOB_URL}"/cab_type_id.json",
  "outputView": "cab_type_id",
  "persist": true
}
```

```json
{
  "type": "JSONExtract",
  "name": "load payment_type_id reference table",
  "environments": ["production", "test"],
  "inputURI": ${ETL_CONF_JOB_URL}"/payment_type_id.json",
  "outputView": "payment_type_id",
  "persist": true
}
```

```json
{
  "type": "JSONExtract",
  "name": "load vendor_id reference table",
  "environments": ["production", "test"],
  "inputURI": ${ETL_CONF_JOB_URL}"/vendor_id.json",
  "outputView": "vendor_id",
  "persist": true
}
```

The main culprit of non-normalized data is the `yellow_tripdata0` dataset so add a [SQLTransform](/transform/#sqltransform) stage which will do a `LEFT JOIN` to the new reference data then a `SQLValidate` stage to check that all of our refrence table lookups were successful (foreign key integrity). The use of a `LEFT JOIN` over an `INNER JOIN` is that we don't want to filter out data from `yellow_tripdata0` that doesn't have a lookup value.

```sql
%sql name="look up reference data" outputView=yellow_tripdata0_enriched environments=production,test
SELECT
  yellow_tripdata0.vendor_name
  ,vendor_id.vendor_id
  ,trip_pickup_datetime AS pickup_datetime
  ,trip_dropoff_datetime AS dropoff_datetime
  ,store_and_fwd_flag
  ,rate_code_id
  ,start_lon AS pickup_longitude
  ,start_lat AS pickup_latitude
  ,end_lon AS dropoff_longitude
  ,end_lat AS dropoff_latitude
  ,passenger_count
  ,trip_distance
  ,fare_amt AS fare_amount
  ,surcharge AS extra
  ,mta_tax
  ,tip_amount
  ,tolls_amount
  ,NULL AS ehail_fee
  ,NULL AS improvement_surcharge
  ,total_amount
  ,LOWER(yellow_tripdata0.payment_type) AS payment_type
  ,payment_type_id.payment_type_id
  ,NULL AS trip_type_id
  ,NULL AS pickup_nyct2010_gid
  ,NULL AS dropoff_nyct2010_gid
  ,NULL AS pickup_location_id
  ,NULL AS dropoff_location_id
FROM yellow_tripdata0
LEFT JOIN vendor_id ON yellow_tripdata0.vendor_name = vendor_id.vendor
LEFT JOIN payment_type_id ON LOWER(yellow_tripdata0.payment_type) = payment_type_id.payment_type
```

... and then `SQLValidate` verifies that there are no missing values. This query will also collect up the distinct list of missing values so they can be logged and added manually added to the lookup table if required.

```sql
%sqlvalidate name="ensure all foreign keys exist" environments=production,test
SELECT
  SUM(null_vendor_id) = 0 AND SUM(null_payment_type_id) = 0 AS valid
  ,TO_JSON(
    NAMED_STRUCT(
      'null_vendor_id', COALESCE(SUM(null_vendor_id), 0)
      ,'null_vendor_name', COLLECT_LIST(DISTINCT null_vendor_name)
      ,'null_payment_type_id', COALESCE(SUM(null_payment_type_id), 0)
      ,'null_payment_type', COLLECT_LIST(DISTINCT null_payment_type)
    )
  ) AS message
FROM (
  SELECT
    CASE WHEN vendor_id IS NULL THEN 1 ELSE 0 END AS null_vendor_id
    ,CASE WHEN vendor_id IS NULL THEN vendor_name ELSE NULL END AS null_vendor_name
    ,CASE WHEN payment_type_id IS NULL THEN 1 ELSE 0 END AS null_payment_type_id
    ,CASE WHEN payment_type_id IS NULL THEN payment_type ELSE NULL END AS null_payment_type
  FROM yellow_tripdata0_enriched
) valid
```

Which will succeed with `"message":{"null_payment_type":[],"null_payment_type_id":0,"null_vendor_id":0,"null_vendor_name":[]}`.

Finally the `trips.sql` needs to be modified to join to `yellow_tripdata0_enriched` instead of `yellow_tripdata0`. See the `trips.sql` file to see an additional hardcoded join to the `cab_type_id` reference table.

A runnable snapshot of this job is available:  `examples/tutorial/4/nyctaxi.ipynb`.

## Applying Machine Learning

There are multiple ways to execute Machine Learning using Arc:

- [HTTPTransform](/transform/#httptransform) allows calling an externally hosted model via HTTP.
- [MLTransform](/transform/#mltransform) allows executing pretrained Spark ML model.
- [TensorFlowServingTransform](/transform/#tensorflowservingtransform) allows calling a model hosted in a TensorFlow Serving container.

Assuming you have executed the job up to stage 4 we will use the `trips.delta` file to train a new Spark ML model to attempt to predict the fare based on other input values. It is suggested to use a SQL statement to perform feature creation as even though SQL is clumsy compared with the brevity of Python Pandas it is automatically parallelizable by Spark and so can easily perform on `1` or `n` CPU cores without modification. It is also very easy to find people who can understand the logic:

```sql
-- enrich the data by:
-- - filtering bad data
-- - one-hot encode hour component of pickup_datetime
-- - one-hot encode dayofweek component of pickup_datetime
-- - calculate duration in seconds
-- - adding flag to indicate whether pickup/dropoff within jfk airport bounding box
SELECT
  *
  ,CAST(HOUR(pickup_datetime) = 0 AS INT) AS pickup_hour_0
  ,CAST(HOUR(pickup_datetime) = 1 AS INT) AS pickup_hour_1
  ,CAST(HOUR(pickup_datetime) = 2 AS INT) AS pickup_hour_2
  ,CAST(HOUR(pickup_datetime) = 3 AS INT) AS pickup_hour_3
  ,CAST(HOUR(pickup_datetime) = 4 AS INT) AS pickup_hour_4
  ,CAST(HOUR(pickup_datetime) = 5 AS INT) AS pickup_hour_5
  ,CAST(HOUR(pickup_datetime) = 6 AS INT) AS pickup_hour_6
  ,CAST(HOUR(pickup_datetime) = 7 AS INT) AS pickup_hour_7
  ,CAST(HOUR(pickup_datetime) = 8 AS INT) AS pickup_hour_8
  ,CAST(HOUR(pickup_datetime) = 9 AS INT) AS pickup_hour_9
  ,CAST(HOUR(pickup_datetime) = 10 AS INT) AS pickup_hour_10
  ,CAST(HOUR(pickup_datetime) = 11 AS INT) AS pickup_hour_11
  ,CAST(HOUR(pickup_datetime) = 12 AS INT) AS pickup_hour_12
  ,CAST(HOUR(pickup_datetime) = 13 AS INT) AS pickup_hour_13
  ,CAST(HOUR(pickup_datetime) = 14 AS INT) AS pickup_hour_14
  ,CAST(HOUR(pickup_datetime) = 15 AS INT) AS pickup_hour_15
  ,CAST(HOUR(pickup_datetime) = 16 AS INT) AS pickup_hour_16
  ,CAST(HOUR(pickup_datetime) = 17 AS INT) AS pickup_hour_17
  ,CAST(HOUR(pickup_datetime) = 18 AS INT) AS pickup_hour_18
  ,CAST(HOUR(pickup_datetime) = 19 AS INT) AS pickup_hour_19
  ,CAST(HOUR(pickup_datetime) = 20 AS INT) AS pickup_hour_20
  ,CAST(HOUR(pickup_datetime) = 21 AS INT) AS pickup_hour_21
  ,CAST(HOUR(pickup_datetime) = 22 AS INT) AS pickup_hour_22
  ,CAST(HOUR(pickup_datetime) = 23 AS INT) AS pickup_hour_23
  ,CAST(DAYOFWEEK(pickup_datetime) = 0 AS INT) AS pickup_dayofweek_0
  ,CAST(DAYOFWEEK(pickup_datetime) = 1 AS INT) AS pickup_dayofweek_1
  ,CAST(DAYOFWEEK(pickup_datetime) = 2 AS INT) AS pickup_dayofweek_2
  ,CAST(DAYOFWEEK(pickup_datetime) = 3 AS INT) AS pickup_dayofweek_3
  ,CAST(DAYOFWEEK(pickup_datetime) = 4 AS INT) AS pickup_dayofweek_4
  ,CAST(DAYOFWEEK(pickup_datetime) = 5 AS INT) AS pickup_dayofweek_5
  ,CAST(DAYOFWEEK(pickup_datetime) = 6 AS INT) AS pickup_dayofweek_6
  ,UNIX_TIMESTAMP(dropoff_datetime) - UNIX_TIMESTAMP(pickup_datetime) AS duration
  ,CASE
      WHEN
          (pickup_latitude < 40.651381
          AND pickup_latitude > 40.640668
          AND pickup_longitude < -73.776283
          AND pickup_longitude > -73.794694)
          OR
          (dropoff_latitude < 40.651381
          AND dropoff_latitude > 40.640668
          AND dropoff_longitude < -73.776283
          AND dropoff_longitude > -73.794694)
      THEN 1
      ELSE 0
  END AS jfk
FROM trips
WHERE trip_distance > 0
AND pickup_longitude IS NOT NULL
AND pickup_latitude IS NOT NULL
AND dropoff_longitude IS NOT NULL
AND dropoff_latitude IS NOT NULL
```

To execute the training load the `examples/tutorial/5/New York City Taxi Fare Prediction SparkML.ipynb` notebook. It is commented and will describe the process to execute the SQL above to prepare the training dataset and train a model.

Once done the model can be executed in the notebook by first executing the feature generation [SQLTransform](/transform/#sqltransform) then executing the model via `MLTransform`:

```json
{
  "type": "SQLTransform",
  "name": "merge enrich the trips dataset to add the machine learning feature columns",
  "environments": ["production", "test"],
  "inputURI": ${ETL_CONF_JOB_URL}"/trips_enriched.sql",
  "outputView": "trips_enriched"
}
```

```json
{
  "type": "MLTransform",
  "name": "apply machine learning prediction model",
  "environments": ["production", "test"],
  "inputURI": ${ETL_CONF_JOB_URL}"/trips_enriched.model",
  "inputView": "trips_enriched",
  "outputView": "trips_scored"
}
```

You can now [Load](/load) the output dataset into a target like `DeltaLakeLoad` or `JDBCLoad` into a caching database for serving.

A runnable snapshot of this job is available:  `examples/tutorial/5/nyctaxi.ipynb`.
