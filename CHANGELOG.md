## Change Log

# 2.1.0

- add `SimilarityJoinTransform` a stage which performs a [fuzzy match](https://en.wikipedia.org/wiki/Approximate_string_matching) and can be used for dataset deduplication or approximate joins.
- add missing types `BooleanList`, `Double`, `DoubleList`, `LongList` to config reader.

**BREAKING**
- change API for `LifecyclePlugin` to pass the stage index and the full job pipeline so that the current and other stages can be accessed in the plugin.

# 2.0.1

- update to [Spark 2.4.4](https://spark.apache.org/releases/spark-release-2-4-4.html).
- update to Scala `2.12.9`

# 2.0.0

Arc 2.0.0 is a major (breaking) change which has been done for multiple reasons:

- to support both `Scala 2.11` and `Scala 2.12` as they are not binary compatible and the Spark project is moving to `Scala 2.12`. Arc will be published for both `Scala 2.11` and `Scala 2.12`.
- to decouple stages/extensions reliant on third-party packages from the main repository so that Arc is not dependent on a library which does not yet support `Scala 2.12` (for example).
- to support first-class plugins by providing a better API to allow the same type-safety when reading the job configuration as the core Arc pipeline stages (in fact all the core stages have been rebuilt as included plugins). This extends to allowing version number specification in stage resolution.

**BREAKING**

**REMOVED**
- remove `AzureCosmosDBExtract` stage. This could be reimplemented as a [Lifecycle Plugin](https://tripl-ai.github.io/arc/extend/#lifecycle-plugins).
- remove `AzureEventHubsLoad` stage. This could be reimplemented as a [Lifecycle Plugin](https://tripl-ai.github.io/arc/extend/#lifecycle-plugins).
- remove `DatabricksDeltaExtract` and `DatabricksDeltaLoad` stages and replace with the open-source [DeltaLake](https://delta.io/) versions (`DeltaLakeExtract` and `DeltaLakeLoad`) implemented https://github.com/tripl-ai/arc-deltalake-pipeline-plugin.
- remove `DatabricksSQLDWLoad`. This could be reimplemented as a  [Lifecycle Plugin](https://tripl-ai.github.io/arc/extend/#lifecycle-plugins).
- remove `bulkload` mode from `JDBCLoad`. Any target specific JDBC behaviours could be implemented by a custom plugins if required.
- remove `user` and `password` from `JDBCExecute` for consistency. Move details to either `jdbcURL` or `params`.
- remove the `Dockerfile` and put it in separate repo: (https://github.com/tripl-ai/docker)

**CHANGES**
- changed `inputURI` field for `TypingTransform` to `schemaURI` to allow addition of `schemaView`.
- add `CypherTransform` and `GraphTransform` stages to support the https://github.com/opencypher/morpheus project (https://github.com/tripl-ai/arc-graph-pipeline-plugin).
- add `MongoDBExtract` and `MongoDBLoad` stages (https://github.com/tripl-ai/arc-mongodb-pipeline-plugin).
- move `ElasticsearchExtract` and `ElasticsearchLoad` to their own repository https://github.com/tripl-ai/arc-elasticsearch-pipeline-plugin.
- move `KafkaExtract`, `KafkaLoad` and `KafkaCommitExecute` to their own repository https://github.com/tripl-ai/arc-kafka-pipeline-plugin.

# 1.15.0

- added `uriField` and `bodyField` to `HTTPExtract` allowing dynamic data to be generated and `POST`ed to endpoints when using an `inputView`.

# 1.14.0

- changed all dependencies to `intransitive()`. All tests pass however this may cause issues. Please raise issue if found.
- removed reliance on `/lib` libraries.
- added `endpoint` and `sslEnabled` variables to `AmazonAccessKey` authentication options to help connect to `Minio` or `Ceph Object Store`.

# 1.13.3

- Arc now available on Maven.
- added configuration flag `ETL_CONF_DISABLE_DEPENDENCY_VALIDATION` and `etl.config.disableDependencyValidation` to disable config dependency graph validation in case of dependency resolution defects.

# 1.13.2

- **FIX** issue where using SQL Common Table Expressions (CTE - `WITH` statements) would break the config dependency graph validation.

# 1.13.1

- added ability to add custom key/value tags to all log messages via `ETL_CONF_TAGS` or `etl.config.tags`.

# 1.13.0

- **BREAKING** added `environments` key to [Dynamic Configuration Plugins](https://tripl-ai.github.io/arc/extend/#dynamic-configuration-plugins) and [Lifecycle Plugins](https://tripl-ai.github.io/arc/extend/#lifecycle-plugins) so they can be enabled/disabled depending on the deloyment environment.
- **BREAKING** [Lifecycle Plugins](https://tripl-ai.github.io/arc/extend/#lifecycle-plugins) now require explicit declaration like [Dynamic Configuration Plugins](https://tripl-ai.github.io/arc/extend/#dynamic-configuration-plugins) by use of the `config.lifecycle` attribute.
- **FIX** fixed issue https://issues.apache.org/jira/browse/SPARK-26995 to Dockerfile.
- **FIX** error reading `Elasticsearch*` configuration parameters due to escaping by Typesafe Config.
- added `AzureCosmosDBExtract` stage.
- added ability to pass `params` to [Lifecycle Plugins](https://tripl-ai.github.io/arc/extend/#lifecycle-plugins).
- rewrote tutorial to use [arc-starter](https://github.com/seddonm1/arc-starter).
- added `failMode` to `BytesExtract` to allow pipeline to continue if missing binary files.
- added `DataFramePrinterLifecyclePlugin` to base image.
- `ARC.run()` now returns the final `Option[DataFrame]` facilitating better [integrations](https://github.com/seddonm1/arc-starter).

# 1.12.2

- **FIX** defect where `sqlParams` in `SQLTransform` stage would throw exception.

# 1.12.1

- **FIX** defect where job config files would not resolve internal subsittution values.

# 1.12.0

- bump to Spark [2.4.3](https://spark.apache.org/releases/spark-release-2-4-3.html).
- bump to OpenJDK `8.212.04-r0` in `Dockerfile`.

# 1.11.1

- **FIX** error reading text file during config stage.
- added support for `AzureDataLakeStorageGen2AccountKey` and `AzureDataLakeStorageGen2OAuth` authentication methods.

# 1.11.0

- **BREAKING** `key` and `value` fields extracted as `binary` type from `KafkaExtract` to be consistent with Spark Streaming schema and is easier to generalise.
- added support for writing `binary` key/value to Kafka using `KafkaLoad`.
- added `inputView`, `inputField` and `avroSchemaURI` to allow parsing of `avro` binary data which does not have an embedded schema for reading from sources like like `KafkaExtract` with a [Kafka Schema Registry](https://www.confluent.io/confluent-schema-registry/).

# 1.10.0

- added `basePath` to relevant `*Extract` do aid with partition discovery.
- added check to ensure no parameters are remaining after `sqlParams` string replacement (i.e. missing `sqlParams`).
- added `failMode` to `HTTPTransform` with default `failfast` (unchanged behaviour).
- added streaming mode support to `HTTPLoad`.
- added `binary` `metadata` type to allow decoding `base64` and `hexadecimal` encodings.
- **CHORE** bumped some JAR versions up to latest.

# 1.9.0

- **FIX** fixed command line arguments which contain equals sign not being parsed correctly
- added `DatabricksSQLDWLoad` stage for bulk loading Azure SQLDW when executing in the [Databricks Runtime](https://databricks.com/product/databricks-runtime) environment.
- added `ElasticsearchExtract` and `ElasticsearchLoad` stages for connecting to [Elasticsearch](https://www.elastic.co/products/elasticsearch) clusters.
- added additional checks for table dependencies when validating the job config.
- added `TextLoad` which supports both `singleFile` and standard partitioned output formats.

# 1.8.0

- added ability to pass job substitution variables via the `spark-submit` command instead of only environment variables. See [Typesafe Config Substitutions](https://github.com/lightbend/config#uses-of-substitutions) for additional information.
- added the `DatabricksDeltaExtract` and `DatabricksDeltaLoad` stages for when executing in the [Databricks Runtime](https://databricks.com/product/databricks-runtime) environment.

# 1.7.1

- added additional logging in `*Transform` to better track partition behaviour.

# 1.7.0

- added `partitionBy` and `numPartitions` to relevant `*Transform` stages to allow finer control of parallelism.
- changed to only perform `HTTPExtract` split result when `batchSize` is greater than 1.

# 1.6.1

- **FIX** fixed exit process when executing within a [Databricks Runtime](https://databricks.com/product/databricks-runtime) environment so the job reports success/failure correctly. [Source](https://docs.databricks.com/user-guide/jobs.html#jar-job-tips).
- **FIX** changed log level for `DynamicConfiguration` return values to `DEBUG` to prevent spilling secrets unless opt-in.

# 1.6.0

- **FIX** fixed defect in `*Extract` where Arc would recalculate metadata columns (`_filename`, `_index` or `_monotonically_increasing_id`) if both `_index` or `_monotonically_increasing_id` missing ignoring `_filename` presence.
- **FIX** `HTTPExtract`, `HTTPTransform` and `HTTPLoad` changed to fail fast and hit HTTP endpoint only once.
- **FIX** `JDBCExecute` was not setting connection `params` if `user` or `password` were not provided.
- changed `DynamicConfiguration` plugins to be HOCON `object` rather than a `string` allowing parameters to be passed in.
- added `logger` object to `DynamicConfiguration` plugins.
- added `customDelimiter` attribute to `DelimitedExtract` and `DelimitedLoad` to be used in conjunction with `delimiter` equal to `Custom`.
- added optional `description` attribute to all stages.
- added `inputField` to `DelimitedExtract` and `JSONExtract` to simplify loading from sources like `HTTPExtract`.
- added `batchSize` and `delimiter` to `HTTPTransform` to allow batching to reduce cost of HTTP overhead.
- bump to Alpine 3.9 in `Dockerfile`.

# 1.5.0

- changed `delimiter` for `DelimitedExtract` and `DelimitedLoad` from `DefaultHive` to `Comma`.
- renamed `BytesExtract` attribute `pathView` to `inputView` for consistency.
- renamed `JDBCExecute` attribute `url` to `jdbcURL` for consistency.
- added `authentication` to `PipelineExecute` to allow reading external pipelines from different sources.
- major rework of error messages when reading job and metadata config files.
- bump to OpenJDK 8.191.12-r0 in `Dockerfile`.

# 1.4.1

- added `inputField` to both `TensorFlowServingTransform` and `HTTPTransform` to allow overriding default field `value`.
- added `ImageExtract` to read image files for machine learning etc.
- added the `minLength` and `maxLength` properties to the `string` metadata type.

# 1.4.0

- bump to Spark [2.4.0](https://spark.apache.org/releases/spark-release-2-4-0.html).
- bump to OpenJDK 8.181.13-r0 in `Dockerfile`.

# 1.3.1

- added additional tutorial job at `/tutorial/starter`.

# 1.3.0

- added support for dynamic runtime configuration via `Dynamic Configuration Plugins`.
- added support for custom stages via `Pipeline Stage Plugins`.
- added support for Spark SQL extensions via custom `User Defined Functions` registration.
- added support for specifying custom `formatters` for `TypingTransform` of integer, long, double and decimal types.
- added `failMode` for `TypingTransform` to allow either `permissive` or `failfast` mode.
- added `inputView` capability to `HTTPExtract` stage.

# 1.2.1

- bump to Spark [2.3.2](https://spark.apache.org/releases/spark-release-2-3-2.html).

# 1.2.0

- added support for Spark [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html).
- added `pathView` property for `BytesExtract` allowing a dataframe of file paths to be provided to dynamically load files.
- added `TextExtract` to read basic text files.
- added `RateExtract` which wraps the Spark [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets) rate source for testing streaming jobs.
- added `ConsoleLoad` which wraps the Spark [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks) console sink for testing streaming jobs.
- added ability for `JDBCLoad` to execute in Spark [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks) mode.

# 1.1.0

- fixed a bug in the `build.sbt` mergeStrategy which incorrectly excluded the `BinaryContentDataSource` registration when `assembly`.
- changed `TensorFlowServingTransform` to require a `responseType` argument.

# 1.0.9

- added the ability to add column level `metadata`.
- added the `MetadataFilterTransform` stage - a stage which allows filtering of columns by their metadata.
- added the `PipelineExecute` stage - a stage which allows embeding other pipelines.
- added the `HTTPTransform` stage - a stage which calls an external API and adds result to Dataframe.
- added support for Google Cloud Storage - prefix: `gs://`
- added support for Azure Data Lake Storage - prefix: `adl://`
- added `partitionColumn` to `JDBCExtract` to support parallel extract.
- added `predicates` to `JDBCExtract` to allow manual partition definitions.
- changed `XMLExtract` to be able to support reading `.zip` files.
- changed `*Extract` to allow input `glob` patterns not just simple `URI`.
- changed `*Extract` to support the schema to be provided as `schemaView`.
- added `BytesExtract` to allow `Array[Bytes]` to be read into a dataframe for things like calling external Machine Learning models.
- refactored `TensorFlowServingTransform` to call the `REST` API (in batches) which is easier due to library conflicts creating `protobuf`.
- create the `UDF` registration mechanism allowing logical extension point for common functions missing from Spark core.

# 1.0.8

- added the `KafkaLoad` stage - a stage which will write a dataset to a designated `topic` in Apache Kafka.
- added the `EXPERIMENTAL` `KafkaExtract` stage - a stage which will read from a designated `topic` in Apache Kafka and produce a dataset.
- added the `KafkaCommitExecute` stage - a stage which allows the commiting of the Kafka offsets to be deferred allowing quasi-transactional behaviour.
- added integration tests for `KafkaLoad` and `KafkaExtract`. more to come.
- added `partitionBy` to `*Extract`.
- added `contiguousIndex` option to file based `*Extract` which allows users to opt out of the expensive `_index` resolution from `_monotonically_increasing_id`.
- added ability to send a `POST` request with `body` in `HTTPExtract`.
- changed hashing function for `DiffTransform` and `EqualityValidate` from inbuilt `hash` to `sha2(512)`.

# 1.0.7

- added the `DiffTransform` stage - a stage which efficiently calculutes the difference between two datasets and produces left, right and intersection output views.
- added logging of `records` and `batches` counts to the `AzureEventHubsLoad`.
- updated the `EqualityValidate` to use the `hash` diffing function as the inbuilt `except` function is very difficult to use in practice. API unchanged.
- updated the `HTTPExecute` stage to not automatically split the response body by newline (`\n`) - this is more in line with expected usecase of REST API endpoints.

# 1.0.6

- updated `AzureEventHubsLoad` to use a SNAPSHOT compiled JAR in `/lib` to get latest changes. this will be changed back to Maven once version is officially released. Also exposed the expontential retry options to API.
- initial testing of a `.zip` reader/writer.

# 1.0.5

- fix a longstanding defect in `TypingTranform` not correctly passing through values which are already of correct type.
- change the `_index` field added to `*Extract` from `monotonically_increasing_id()` to a `row_number(monotonically_increasing_id())` so that the index aligns to underlying files and is more intuitive to use.

# 1.0.4

- allow passing of same metadata schema to `JSONExtract` and `XMLExtract` to reduce cost of schema inference for large number of files.

# 1.0.3

- Expose numPartitions Optional Parameter for `*Extract`.

# 1.0.2

- add sql validation step to `SQLValidate` configuration parsing and ensure parameters are injected first (including `SQLTransform`) so the statements with parameters can be parsed.

# 1.0.1

- bump to Spark [2.3.1](https://spark.apache.org/releases/spark-release-2-3-1.html).

# 1.0.0

- initial release.