Arc is an opinionated framework for defining data pipelines which are predictable, repeatable and manageable.

## Documentation

Full documentation is available here: https://arc.tripl.ai

## Index

- [What is Arc?](#what-is-spark-etl-pipeline)
- [Getting Started](#getting-started)
- [Principles](#principles)
- [Not just for data engineers](#not-just-for-data-engineers)
- [Why abstract from code?](#why-abstract-from-code)
- [Why SQL first?](#why-sql-first)
- [Example pipeline](#example-pipeline)
- [Building](#building)
- [Contributing](#contributing)
- [License](#license)

## What is Arc?

Arc is an **opinionated** framework for defining **predictable**, **repeatable** and **manageable** data transformation pipelines;

- **predictable** in that data is used to define transformations - not code.
- **repeatable** in that if a job is executed multiple times it will produce the same result.
- **manageable** in that execution considerations and logging have been baked in from the start.

## Getting Started

![Notebook](/docs-src/static/img/arc-starter.png)

Arc has an interactive [Jupyter Notebook](https://jupyter.org/) extension to help with rapid development of jobs. Start by cloning [https://github.com/tripl-ai/arc-starter](https://github.com/tripl-ai/arc-starter) and running through the [tutorial](https://arc.tripl.ai/tutorial/).

This extension is available at [https://github.com/tripl-ai/arc-jupyter](https://github.com/tripl-ai/arc-jupyter).

## Principles

Many of these principles have come from [12factor](https://12factor.net/) with the aim of deploying **predictable**, **repeatable** and **manageable** data transformation pipelines:

- **[single responsibility](https://en.wikipedia.org/wiki/Single_responsibility_principle)** components/stages with first-class support for [extending](https://arc.tripl.ai/extend/).
- **stateless** jobs where possible and use of [immutable](https://en.wikipedia.org/wiki/Immutable_object) datasets.
- **precise logging** to allow management of jobs at scale.
- **library dependencies** limited or avoided where possible.

## Not just for data engineers

The intent of the pipeline is to provide a simple way of creating Extract-Transform-Load (ETL) pipelines which are able to be maintained in production, and captures the answers to simple operational questions transparently to the user.

- **monitoring**: is it working each time it's run? and how much resource was consumed in creating it?
- **devops**: is packaged as a Docker image to allow rapid deployment on ephemeral compute.

These concerns are supported at run time to ensure that as deployment grows in uses and complexity it does not become opaque and unmanageable.

## Why abstract from code?

From experience a very high proportion of data pipelines perform very similar extract, transform and load actions on datasets. Unfortunately, whilst the desired outcomes are largely similar, the implementations are vastly varied resulting in higher maintenance costs, lower test-coverage and high levels of rework.

The intention of this project is to define and implement an **opinionated** standard approach for declaring data pipelines which is open and extensible. Abstraction from underlying code allows rapid deployment, a consistent way of defining transformation tasks (such as data typing) and allows abstraction of the pipeline definition from the pipeline execution (to support changing of the underlying execution engines) - see [declarative programming](https://en.wikipedia.org/wiki/Declarative_programming).

Currently it is tightly coupled to [Apache Spark](https://spark.apache.org) due to its fault-tolerance, performance and solid API for standard data engineering tasks but the definitions are human and machine readable [HOCON](https://github.com/lightbend/config/blob/master/HOCON.md) (a JSON derivative) allowing the transformation definitions to be implemented against future execution engines.

## Why SQL first?

SQL first (copied from the Mobile First UX principle) is an approach where, if possible, transformations are done using Structured Query Language (SQL) as a preference. This is because SQL is a very good way of expressing standard data transformation intent in a [declarative](https://en.wikipedia.org/wiki/Declarative_programming) way. SQL is so widely known and taught that finding people who are able to understand the business context and able to write basic SQL is much easier than finding a Scala developer who also understands the business context (for example).

Currently the [HIVE](https://cwiki.apache.org/confluence/display/Hive/LanguageManual) dialect of SQL is supported as [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) uses the same SQL dialect and has a lot of the same [functions](https://spark.apache.org/docs/latest/api/sql/) that would be expected from other SQL dialects. This could change in the future.

## Example pipeline

This is an example of a fairly standard pipeline:

1. First load a set of CSV files from an input directory. Separator is a comma and the file has a header.

2. Convert the data to the correct datatypes using metadata defined in a separate JSON.

3. Execute a SQL statement that will perform custom validation to ensure the data conversion in the previous step resulted in an acceptable data conversion error rate. If not successful the job will terminate.

4. Write out the aggreate resultset to a Parquet target.

```json
{
  "stages": [
    {
      "type": "DelimitedExtract",
      "name": "extract data from green_tripdata/0",
      "environments": [
        "production",
        "test"
      ],
      "inputURI": ${ETL_CONF_BASE_URL}"/data/green_tripdata/0/*",
      "outputView": "green_tripdata0_raw",
      "delimiter": "Comma",
      "quote": "DoubleQuote",
      "header": true
    },
    {
      "type": "TypingTransform",
      "name": "apply green_tripdata/0 data types",
      "environments": [
        "production",
        "test"
      ],
      "inputURI": ${ETL_CONF_BASE_URL}"/meta/green_tripdata/0/green_tripdata.json",
      "inputView": "green_tripdata0_raw",
      "outputView": "green_tripdata0",
      "persist": true
    },
    {
      "type": "SQLValidate",
      "name": "ensure no errors exist after data typing",
      "environments": [
        "production",
        "test"
      ],
      "inputURI": ${ETL_CONF_BASE_URL}"/job/0/sqlvalidate_errors.sql",
      "sqlParams": {
        "table_name": "green_tripdata0"
      }
    },
    {
      "type": "ParquetLoad",
      "name": "write trips back to filesystem",
      "environments": [
        "production",
        "test"
      ],
      "inputView": "green_tripdata0",
      "outputURI": ${ETL_CONF_BASE_URL}"/data/output/green_tripdata0.parquet",
      "saveMode": "Overwrite"
    }
  ]
}
```

A full worked example job is available [here](https://github.com/tripl-ai/arc/tree/master/tutorial/gnaf/job/0). It is for the Australian Geocoded National Address File [G-NAF](https://data.gov.au/dataset/geocoded-national-address-file-g-naf) and will load the pipe-separated data, apply data types, ensure there are no errors in applying data types and write the output to Parquet. This could easily be modified to write to a JDBC target.

## Building

### Library

To compile the main library run:

```bash
sbt +package
```

To build a library to use with a [Databricks Runtime](https://databricks.com/product/databricks-runtime) environment it is easiest to `assembly` Arc with all the dependencies into a single JAR to simplify the deployment.

Example:

```bash
sbt assembly
```

If you are having problems compiling it is likely due to environment setup. This command is executed in CICD and uses a predictable build environment pulled from Dockerhub:

```bash
docker run --rm -v $(pwd):/app -w /app mozilla/sbt:8u212_1.2.8 sbt +package
```

### Tests

To run unit tests:

```bash
sbt +test
```

To run integration tests (which have external service depenencies):

```bash
./it.sh
```

### License Report

To rebuild the license report run:

```bash
sbt dumpLicenseReport
```

### Documentation

To generate the documentation you need to download [Hugo](https://gohugo.io/) to `/docs-src` and run `./hugo` in that  directory. The `/docs` directory is the output of the docuementation generation and should not be edited by hand. The `/docs` directory is automatically published by the Github Pages process on commit.

You can also generate documentation for previous version by checking out a previous tag (`git checkout tags/1.3.0`) and running `hugo server` from the `/docs-src` directory.

## Contributing

If you have suggestions of additional components or find issues that you believe need fixing then please raise an issue. An issue with a test case is even more appreciated.

When you contribute code, you affirm that the contribution is your original work and that you license the work to the project under the project’s open source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project’s open source license and warrant that you have the legal authority to do so.

## Support

For questions around use (which are not clear from the documentation) post a new 'issue' in the [questions](https://github.com/tripl-ai/questions/issues) repository. This repository acts as a forum where questions can be posted, discussed and searched.

For commercial support requests please [contact us](mailto:contact@tripl.ai) via email.

## Authors/Contributors

- [Mike Seddon](https://github.com/seddonm1)
- [John Bruce](https://github.com/jbruce)


## Attribution

Thanks to the following projects:

- [AGL Energy](https://github.com/aglenergy) for the original development.
- [Apache Spark](https://spark.apache.org/) for the underlying framework that has made this library possible.
- [slf4j-json-logger](https://github.com/savoirtech/slf4j-json-logger) Copyright (c) 2016 Savoir Technologies released under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0). We have slightly altered their library to change the default logging format.
- [nyc-taxi-data](https://github.com/toddwschneider/nyc-taxi-data) for preparing an easy to use set of real-world data for the tutorial.

## License

Arc is released under the [MIT License](https://opensource.org/licenses/MIT).

