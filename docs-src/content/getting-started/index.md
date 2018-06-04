---
title: Getting started
weight: 10
type: blog
---

## Directed Acyclic Graphs

A pipeline is a [Directed Acyclic Graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph) specified using a `json` file format with the sequence of transformation stages defined in the `stages` array.

Consider the following data pipeline:

![Data Pipeline Dag](/img/drawio_pipeline_dag.png)

The job logically executes in the following way:

### Branch One (in parallel with Branch Two)

1. The [DelimitedExtract](../extract/#delimitedextract) stage loads `Comma` separated files from `hdfs://datalake/raw/customer/*.csv` and named `customerUntypedDF` internally.
1. The [TypingTransform](../transform/#typingtransform) stage takes the `customerUntypedDF` dataset and performs data typing on it using the [metadata](../metadata/) defined at `hdfs://datalake/meta/0.0.1/customer_meta.json`. The resulting dataset is named `customerTypeDF` internally. 
1. The [ParquetLoad](../load/#parquetload) stage writes the `customerTypeDF` dataset to `hdfs://datalake/output/customer.parquet`.

### Branch Two (in parallel with Branch One)

1. The [ParquetExtract](../extract/#parquetextract) stage loads the Parquet dataset from `hdfs://datalake/raw/account.parquet` and named `accountDF` internally.

### Once both of those stages are complete:

1. The [SQLTransform](../transform/#sqltransform) stage executes the script located at `hdfs://datalake/sql/0.0.1/customerAccountJoin.sql` to create a new dataset named `customerAccountDF` internally. For this to execute correctly the SQL must refer to the datasets by their internal names `customerTypeDF` and `accountDF` as the table names.

### Branch One (in parallel with Branch Two)

1. The [ParquetLoad](../load/#parquetload) stage takes the `customerAccountDF` dataset and writes it to `hdfs://datalake/output/customerAccount.parquet`.

### Branch Two (in parallel with Branch One)

1. The [JDBCExecute](../execute/#jdbcexecute) stages executes the script from `hdfs://datalake/sql/0.0.1/update.sql` against a Microsoft Azure SQLDW JDBC connection.

## Execution Model

Internally Spark uses the [spark-driver](https://spark.apache.org/docs/latest/cluster-overview.html#cluster-mode-overview) process to co-ordinate the execution of these stages. Where possible the driver will distribute certain tasks within stages to other machines in the cluster for execution efficiency but each stage will ultimately execute sequentially: 

![Data Pipeline Linear](/img/drawio_pipeline_linear.png)

## Job Specification Format