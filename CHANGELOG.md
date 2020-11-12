# Change Log

## 3.6.1

- add `JupyterCompleter` to Lifecycle Plugins `ChaosMonkey` and `ControlFlow`.

## 3.6.0

- add `ControlFlowExecute` and `ControlFlow` plugins to support work avoidance.

## 3.5.3

- **FIX** remove limitation that required field `metadata` names to be different to the field `name`.

## 3.5.2

- **FIX** reorder AWS Identity and Access Management providers `com.amazonaws.auth.WebIdentityTokenCredentialsProvider` to allow use of Arc on Amazon Elastic Kubernetes Service with IAM.

## 3.5.1

- **FIX** issue with `TensorFlowServingTransform` transform not parsing `batchSize` argument correctly.

## 3.5.0

- add `StatisticsExtract` stage.
- add `etl.config.lintOnly` (`ETL_CONF_LINT_ONLY`) option to only validate the configuration and not run the job.
- add validation that stage `id` values are unique within the job.
- **FIX** minor defect relating to order of `etl.config.uri` vs `etl.config.environments` error messages.

## 3.4.1

- set name on Dataframe when `persist=true` to help understand persisted datasets when using the Spark UI.
- add better error messages to job failure if `lazy` evaluation is set.
- add support for `%configexecute` from Arc Jupyter notebooks.

## 3.4.0

- add `resolution` tag for all stages to indicate `lazy` or `strict` resolution of stage variables. This can be used with `ConfigExecute` to generate runtime specific configuration variables.
- add `ConfigExecute` stage to allow dynamic creation of runtime variables.
- **FIX** revert `DiffTransform` and `EqualityValidate` to use `sha2(to_json(struct()))` rather than inbuilt Spark `hash` function due to high liklihood of collisions.

## 3.3.3

- **FIX** standardise `persist` behavior logic in `DiffTransform` to match other stages.

## 3.3.2

- **FIX** incorrect logic in `DiffTransform` and bad test for changes made in `3.3.1`.

## 3.3.1

- **FIX** `MetadataExtract` to export full Arc schema (a superset of the previous schema) so that it can be used with `schemaView`.
- **FIX** `DiffTransform` will output the `left` and `right` structs in the `intersectionView` when `inputLeftKeys` or `inputRightKeys` are supplied.

## 3.3.0

- bump to Spark [3.0.1](https://spark.apache.org/releases/spark-release-3-0-1.html).
- add `inputLeftKeys` and `inputRightKeys` to `DiffTransform` to support matching on a subset of keys.

## 3.2.0

- rewrote the code to calculate `_index` (vs `_monotonically_increasing_id`) to be more efficient with large datasets.
- add optional `id` attribute to all stages which can be used to easily identify individual stages when monitoring logs.
- add the `schema` attribute to `TypingTransform` and `MetadataTransform` to allow inline schema. Can be disabled via `etl.policy.inline.schema` and `ETL_POLICY_INLINE_SCHEMA`. These are being trialed and if beneficial will be added to all stages that support schemas.
- rename `etl.policy.inlinesql` to `etl.policy.inline.sql` and `ETL_POLICY_INLINESQL` to `ETL_POLICY_INLINE_SQL`.
- remove forced use of `etl.config.fs.gs.project.id`/`ETL_CONF_GOOGLE_CLOUD_PROJECT_ID` and `etl.config.fs.google.cloud.auth.service.account.json.keyfile`/`ETL_CONF_GOOGLE_CLOUD_AUTH_SERVICE_ACCOUNT_JSON_KEYFILE` to access Google Cloud Storage job files.
- remove previous optimisation when reading a large number of small `json` files in `JSONExtract`. This is to better align with `DataSourceV2`.
- added `sql` attribute to `MetadataFilterTransform` and `MetadataValidate` allowing inline SQL statements.
- added support for scientific notation to `Integer` and `Long` when performing `TypingTransform`.
- **FIX** a non-threadsafe HashMap was used in string validation functions resulting in non-deterministic hanging in the `TypingTransform`. This would happen more frequently with datasets containing many string columns.
- This commit replaces the HashMap with the threadsafe ConcurrentHashMap
- **BREAKING** disable automatic dropping of unsupported types when performing `*Load` stages (e.g. `ParquetLoad` cannot support `NullType`). Old behavior can be enabled by setting `etl.policy.drop.unsupported`/`ETL_POLICY_DROP_UNSUPPORTED` to `true`.
- **BREAKING** remove deprecated `etl.config.environment.id` and `ETL_CONF_ENV_ID` in favor of `etl.config.tags` or `ETL_CONF_TAGS`.

## 3.1.1

- remove `spark.authenticate.secret` from log output.
- support nested `struct` and `array` types in `makeMetadataFromDataframe` helper function used by Arc Jupyter `%printmetadata` magic.
- minor tweaks to readers and writers to begin `DataSourceV2` support.

## 3.1.0

- add the `JupyterCompleter` trait for auto-completion in Jupyter allowing `snippet`, `language` and `documentationURI` to be defined.
- add the `ExtractPipelineStage`, `TransformPipelineStage` and `LoadPipelineStage` traits to allow easier pattern matching in `LifecyclePlugins`.

## 3.0.0

- bump to Spark [3.0.0](https://spark.apache.org/releases/spark-release-3-0-0.html).
- bump to Hadoop [3.2.0](https://hadoop.apache.org/release/3.2.0.html).
- **FIX** `MLTransform` dropping all calculated columns when applying models which do not produce a prediction column.
- **BREAKING** remove `Scala 2.11` support as Arc is now built against `Spark 3.0.0` which does not support `Scala 2.11`.
- **BREAKING** move `XMLExtract` and `XMLLoad` to [arc-xml-plugin](https://github.com/triplai/arc-xml-plugin).
- **BREAKING** Spark ML models trained with Spark 2.x do not work with Spark 3.x and will need to be retrained (`MLTransform`).
- **BREAKING** remove `GraphTransform` and `CypherTransform` as the underlying [library](https://github.com/opencypher/morpheus) has been [abandoned](https://github.com/opencypher/morpheus/issues/943#issuecomment-610215881).

## 2.14.0

**This is the last release supporting `Scala 2.s11` given the release of `Spark 3.0` which only supports `Scala 2.12`.**

- add support for case-insensitive formatter (default `true`) to allow formatter `MMM` to accept `JUL` and `Jul` where case-sensitive will only accept `Jul`. Applies to `Date` and `Timestamp` schema columns. Boolean property `caseSensitive` can be used to set case-sensitive behavior.

## 2.13.0

- bump to Spark [2.4.6](https://spark.apache.org/releases/spark-release-2-4-6.html).

## 2.12.5

- **FIX** support for `timestamp` `formatters` that include offset (e.g. `+01:00`) will override `timezoneId` which remains mandatory.

## 2.12.4

- **FIX** rare edge-case of `TextLoad` in `singleFile` mode throwing non-seriaizable exception when non-serializable `userData` exists.

## 2.12.3

- add support for parsing `array` objects when returned in the `message` field from `LogExecute` and `SQLValidate`.

## 2.12.2

- **FIX** `PipelineExecute` long standing issue where errors in nested pipeline were not being exposed correctly.
- **FIX** restore the ability for user to be able to define `fs.s3a.aws.credentials.provider` overrides via `--conf spark.hadoop.fs.s3a.aws.credentials.provider=`

## 2.12.1

- **FIX** `PipelineExecute` so that it will correctly identify `.ipynb` files and parse them correctly.
- add `get_uri_filename_array` user defined function which returns the contents of a Glob/URI as an `Array[(Array[Byte], String)]` where the second return value is `filename`.
- remove `delay` versions of `get_uri` as this can be handled by target service.

## 2.12.0

- **FIX** prevent early validation failure of SQL statements which contain `${hiveconf:` or `${hivevar:`.
- add `get_uri_array` user defined function which returns the contents of a Glob/URI as an `Array[Array[Byte]]`.
- add `get_uri_array_delay` which is the same as `get_uri_array` but adds a delay in milliseconds to reduce DDOS liklihood.
- add `LogExecute` which allows logging to the Arc log similar to `SQLValidate` but without the success/fail decision.

## 2.11.0

- add ability to define the Arc [schema](https://arc.tripl.ai/schema/) with a `schema` key (`{"schema": [...]}`) so that common attributes can be defined using [Human-Optimized Config Object Notation](https://en.wikipedia.org/wiki/HOCON) (HOCON) functionality.
- add ability to define `regex` when parsing string columns to perform validation.
- remove mandatory requirement to supply `trim`, `nullReplacementValue` and `nullableValues` for [schema](https://arc.tripl.ai/schema/) that don't logically use them. This will not break existing configurations.
- change `DiffTransform` and `EqualityValidate` to use inbuilt Spark `hash` function rather than `sha2(to_json(struct()))`.
- add `get_uri_delay` which is the same as `get_uri` but adds a delay in milliseconds to reduce DDOS liklihood.

## 2.10.2

- **FIX** `get_uri` ability to read compressed file formats `.gzip`, `.bzip2`, `.lz4`.
- add `get_uri` ability to read from `http` and `https` sources.

## 2.10.1

- add the `get_uri` user defined function which returns the contents of a URI as an `Array[Byte]` which can be used with `decode` to convert to text.
- rename `frameworkVersion` to `arcVersion` in initiliasation logs for clarity.

## 2.10.0

- make `id` an optional field when specifying an Arc Schema.
- make `TextLoad` `singleFile` mode load in parallel from the Spark `executor` processes rather than `driver` as they will have likely have more ram available.
- add option to supply `index` in addition to [`value`, `filename`] to ensure correct output ordering when in `singleFile` mode.
- add `singleFile` mode to `XMLLoad`.
- add `to_xml` UDF.
- add support for `struct` and `array` types in the schema definition file.
- add support for `TextExtract` to be suppiled an `inputView`.
- **FIX** any deprecations preventing upgrade to Spark 3.0.
- deprecate of `get_json_double_array`, `get_json_integer_array`, `get_json_long_array` in favor of inbuilt `get_json_object`.
- ability to pass in config file via `http` or `https`.
- **BREAKING** remove `DataFramePrinter` lifecycle plugin as it presents too much risk of data leakage.
- **BREAKING** Amazon Web Services `authentication` methods will now limit their scope to specific buckets rather than global.
- **BREAKING** remove ability to read `.zip` files.

## 2.9.0

- **FIX** defect with `JSONExtract` not using `basePath` correctly.
- add ability to export to multiple files by providing second `filename` column to `TextLoad` when set to `singleFile` mode but not create a directory like the standard Spark behavior.
- add ability to provide an [XML Schema Definition](https://en.wikipedia.org/wiki/XML_Schema_(W3C)) (XSD) to `XMLExtract` to validate input file schemas.
- add `blas` and `lapack` implementation to `MLTransform` logs to help debug performance.

## 2.8.1

- **FIX** defect with `MetadataTransform` logic.
- add `contentLength` to `HTTPExtract` response and logs.

## 2.8.0

- **FIX** defect which reported job failure when running in YARN mode.
- **FIX** defect relating to Amazon S3 protocol to use when running on [Amazon EMR](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-file-systems.html)
- added `sql` attribute to `SQLTransform` and `SQLValidate` allowing inline SQL statements.
- added capability to replace the returned dataset via a `LifecyclePlugin` `after` hook.
- removed `tutorials` directory from main arc repo as it is available via [arc-starter](https://github.com/tripl-ai/arc-starter).
- bump to Spark [2.4.5](https://spark.apache.org/releases/spark-release-2-4-5.html).

## 2.7.0

- added `ContainerCredentials` provider to resolver list allowing IAM roles to be accessed by Arc jobs running inside an Amazon ECS container (specified via `taskRoleArn` in ECS).
- added `AmazonAnonymous` mode to default provider list meaning users do not have to specify it manually.
- enhanced file based `*Extract` to throw richer error messages when files are not found.

## 2.6.0

- provided ability for job configuration files to be retrieved via `AmazonIAM` (by default) in addition to the existing `AccessKey` and `AmazonAnonymous` methods.

## 2.5.0

- enhanced `PipelineExecute` to allow execution of nested [Lifecycle Plugins](https://arc.tripl.ai/plugins/#lifecycle-plugins).
- changed Stack Trace logging to be opt-in when errors occur (default `false`) via parameters `ETL_CONF_ENABLE_STACKTRACE` and `etl.config.enableStackTrace`.

## 2.4.0

- **FIX** defect in `DelimitedExtract` when running in streaming mode.
- added `MetadataExtract` stage which creates an Arc `metadata` dataframe from an input view.
- added `MetadataTransform` stage which attaches/overrides the metadata attached to an input view.
- added `MetadataValidate` stage which allows runtime rules to be aplied against an input view's metadata.
- added ability to include `%configplugins` when defined [arc-jupyter](https://github.com/tripl-ai/arc-jupyter) notebook files (.ipynb).
- rewrote tutorial to point to public datasets rather than requiring user to download data first.

## 2.3.1

- will now throw exceptions if trying to use the Amazon `s3://` or `s3n://` protocols instead of `s3a://` as they have been deprecated by the Hadoop project, are no longer suppored in Hadoop 3.0+ and do not behave predictably with the Arc Amazon [Authentication](https://arc.tripl.ai/partials/#authentication) methods.
- will now throw clearer message when typing conversion fails on a non-nullable field (which means the job cannot logically proceed).

## 2.3.0

- added `AmazonAnonymous` (for public buckets) and `AmazonIAM` (allowing users to specify encryption method) [Authentication](https://arc.tripl.ai/partials/#authentication) methods.
- added initial support for `runStage` to [Lifecycle Plugins](https://arc.tripl.ai/plugins/#lifecycle-plugins) to support early job exit with success when certain criteria are met.

## 2.2.0

- add ability to execute [arc-jupyter](https://github.com/tripl-ai/arc-jupyter) notebook files (.ipynb) directly without conversion to arc 'job'.
- add `watermark` to `DelimitedExtract`, `ImageExtract`, `JSONExtract`, `ORCExtract`, `ParquetExtract` and `TextExtract` for structured streaming.
- performance and usability improvements for `SimilarityJoinTransform`. Can now cope with null inputs and performs caching to prevent recalculation of input data.
- **FIX** issue where malformed job configuration files would not error and job would exit with success.

## 2.1.0

- add `SimilarityJoinTransform` a stage which performs a [fuzzy match](https://en.wikipedia.org/wiki/Approximate_string_matching) and can be used for dataset deduplication or approximate joins.
- add missing types `BooleanList`, `Double`, `DoubleList`, `LongList` to config reader.

**BREAKING**

- change API for `LifecyclePlugin` to pass the stage index and the full job pipeline so that the current and other stages can be accessed in the plugin.

## 2.0.1

- update to [Spark 2.4.4](https://spark.apache.org/releases/spark-release-2-4-4.html).
- update to Scala `2.12.9`

## 2.0.0

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

## 1.15.0

- added `uriField` and `bodyField` to `HTTPExtract` allowing dynamic data to be generated and `POST`ed to endpoints when using an `inputView`.

## 1.14.0

- changed all dependencies to `intransitive()`. All tests pass however this may cause issues. Please raise issue if found.
- removed reliance on `/lib` libraries.
- added `endpoint` and `sslEnabled` variables to `AmazonAccessKey` authentication options to help connect to `Minio` or `Ceph Object Store`.

## 1.13.3

- Arc now available on Maven.
- added configuration flag `ETL_CONF_DISABLE_DEPENDENCY_VALIDATION` and `etl.config.disableDependencyValidation` to disable config dependency graph validation in case of dependency resolution defects.

## 1.13.2

- **FIX** issue where using SQL Common Table Expressions (CTE - `WITH` statements) would break the config dependency graph validation.

## 1.13.1

- added ability to add custom key/value tags to all log messages via `ETL_CONF_TAGS` or `etl.config.tags`.

## 1.13.0

- **BREAKING** added `environments` key to [Dynamic Configuration Plugins](https://tripl-ai.github.io/arc/extend/#dynamic-configuration-plugins) and [Lifecycle Plugins](https://tripl-ai.github.io/arc/extend/#lifecycle-plugins) so they can be enabled/disabled depending on the deloyment environment.
- **BREAKING** [Lifecycle Plugins](https://tripl-ai.github.io/arc/extend/#lifecycle-plugins) now require explicit declaration like [Dynamic Configuration Plugins](https://tripl-ai.github.io/arc/extend/#dynamic-configuration-plugins) by use of the `config.lifecycle` attribute.
- **FIX** fixed issue https://issues.apache.org/jira/browse/SPARK-26995 to Dockerfile.
- **FIX** error reading `Elasticsearch*` configuration parameters due to escaping by Typesafe Config.
- added `AzureCosmosDBExtract` stage.
- added ability to pass `params` to [Lifecycle Plugins](https://tripl-ai.github.io/arc/extend/#lifecycle-plugins).
- rewrote tutorial to use [arc-starter](https://github.com/tripl-ai/arc-starter).
- added `failMode` to `BytesExtract` to allow pipeline to continue if missing binary files.
- added `DataFramePrinterLifecyclePlugin` to base image.
- `ARC.run()` now returns the final `Option[DataFrame]` facilitating better [integrations](https://github.com/tripl-ai/arc-starter).

## 1.12.2

- **FIX** defect where `sqlParams` in `SQLTransform` stage would throw exception.

## 1.12.1

- **FIX** defect where job config files would not resolve internal subsittution values.

## 1.12.0

- bump to Spark [2.4.3](https://spark.apache.org/releases/spark-release-2-4-3.html).
- bump to OpenJDK `8.212.04-r0` in `Dockerfile`.

## 1.11.1

- **FIX** error reading text file during config stage.
- added support for `AzureDataLakeStorageGen2AccountKey` and `AzureDataLakeStorageGen2OAuth` authentication methods.

## 1.11.0

- **BREAKING** `key` and `value` fields extracted as `binary` type from `KafkaExtract` to be consistent with Spark Streaming schema and is easier to generalise.
- added support for writing `binary` key/value to Kafka using `KafkaLoad`.
- added `inputView`, `inputField` and `avroSchemaURI` to allow parsing of `avro` binary data which does not have an embedded schema for reading from sources like like `KafkaExtract` with a [Kafka Schema Registry](https://www.confluent.io/confluent-schema-registry/).

## 1.10.0

- added `basePath` to relevant `*Extract` do aid with partition discovery.
- added check to ensure no parameters are remaining after `sqlParams` string replacement (i.e. missing `sqlParams`).
- added `failMode` to `HTTPTransform` with default `failfast` (unchanged behaviour).
- added streaming mode support to `HTTPLoad`.
- added `binary` `metadata` type to allow decoding `base64` and `hexadecimal` encodings.
- **CHORE** bumped some JAR versions up to latest.

## 1.9.0

- **FIX** fixed command line arguments which contain equals sign not being parsed correctly
- added `DatabricksSQLDWLoad` stage for bulk loading Azure SQLDW when executing in the [Databricks Runtime](https://databricks.com/product/databricks-runtime) environment.
- added `ElasticsearchExtract` and `ElasticsearchLoad` stages for connecting to [Elasticsearch](https://www.elastic.co/products/elasticsearch) clusters.
- added additional checks for table dependencies when validating the job config.
- added `TextLoad` which supports both `singleFile` and standard partitioned output formats.

## 1.8.0

- added ability to pass job substitution variables via the `spark-submit` command instead of only environment variables. See [Typesafe Config Substitutions](https://github.com/lightbend/config#uses-of-substitutions) for additional information.
- added the `DatabricksDeltaExtract` and `DatabricksDeltaLoad` stages for when executing in the [Databricks Runtime](https://databricks.com/product/databricks-runtime) environment.

## 1.7.1

- added additional logging in `*Transform` to better track partition behaviour.

## 1.7.0

- added `partitionBy` and `numPartitions` to relevant `*Transform` stages to allow finer control of parallelism.
- changed to only perform `HTTPExtract` split result when `batchSize` is greater than 1.

## 1.6.1

- **FIX** fixed exit process when executing within a [Databricks Runtime](https://databricks.com/product/databricks-runtime) environment so the job reports success/failure correctly. [Source](https://docs.databricks.com/user-guide/jobs.html#jar-job-tips).
- **FIX** changed log level for `DynamicConfiguration` return values to `DEBUG` to prevent spilling secrets unless opt-in.

## 1.6.0

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

## 1.5.0

- changed `delimiter` for `DelimitedExtract` and `DelimitedLoad` from `DefaultHive` to `Comma`.
- renamed `BytesExtract` attribute `pathView` to `inputView` for consistency.
- renamed `JDBCExecute` attribute `url` to `jdbcURL` for consistency.
- added `authentication` to `PipelineExecute` to allow reading external pipelines from different sources.
- major rework of error messages when reading job and metadata config files.
- bump to OpenJDK 8.191.12-r0 in `Dockerfile`.

## 1.4.1

- added `inputField` to both `TensorFlowServingTransform` and `HTTPTransform` to allow overriding default field `value`.
- added `ImageExtract` to read image files for machine learning etc.
- added the `minLength` and `maxLength` properties to the `string` metadata type.

## 1.4.0

- bump to Spark [2.4.0](https://spark.apache.org/releases/spark-release-2-4-0.html).
- bump to OpenJDK 8.181.13-r0 in `Dockerfile`.

## 1.3.1

- added additional tutorial job at `/tutorial/starter`.

## 1.3.0

- added support for dynamic runtime configuration via `Dynamic Configuration Plugins`.
- added support for custom stages via `Pipeline Stage Plugins`.
- added support for Spark SQL extensions via custom `User Defined Functions` registration.
- added support for specifying custom `formatters` for `TypingTransform` of integer, long, double and decimal types.
- added `failMode` for `TypingTransform` to allow either `permissive` or `failfast` mode.
- added `inputView` capability to `HTTPExtract` stage.

## 1.2.1

- bump to Spark [2.3.2](https://spark.apache.org/releases/spark-release-2-3-2.html).

## 1.2.0

- added support for Spark [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html).
- added `pathView` property for `BytesExtract` allowing a dataframe of file paths to be provided to dynamically load files.
- added `TextExtract` to read basic text files.
- added `RateExtract` which wraps the Spark [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets) rate source for testing streaming jobs.
- added `ConsoleLoad` which wraps the Spark [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks) console sink for testing streaming jobs.
- added ability for `JDBCLoad` to execute in Spark [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks) mode.

## 1.1.0

- fixed a bug in the `build.sbt` mergeStrategy which incorrectly excluded the `BinaryContentDataSource` registration when `assembly`.
- changed `TensorFlowServingTransform` to require a `responseType` argument.

## 1.0.9

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

## 1.0.8

- added the `KafkaLoad` stage - a stage which will write a dataset to a designated `topic` in Apache Kafka.
- added the `EXPERIMENTAL` `KafkaExtract` stage - a stage which will read from a designated `topic` in Apache Kafka and produce a dataset.
- added the `KafkaCommitExecute` stage - a stage which allows the commiting of the Kafka offsets to be deferred allowing quasi-transactional behaviour.
- added integration tests for `KafkaLoad` and `KafkaExtract`. more to come.
- added `partitionBy` to `*Extract`.
- added `contiguousIndex` option to file based `*Extract` which allows users to opt out of the expensive `_index` resolution from `_monotonically_increasing_id`.
- added ability to send a `POST` request with `body` in `HTTPExtract`.
- changed hashing function for `DiffTransform` and `EqualityValidate` from inbuilt `hash` to `sha2(512)`.

## 1.0.7

- added the `DiffTransform` stage - a stage which efficiently calculutes the difference between two datasets and produces left, right and intersection output views.
- added logging of `records` and `batches` counts to the `AzureEventHubsLoad`.
- updated the `EqualityValidate` to use the `hash` diffing function as the inbuilt `except` function is very difficult to use in practice. API unchanged.
- updated the `HTTPExecute` stage to not automatically split the response body by newline (`\n`) - this is more in line with expected usecase of REST API endpoints.

## 1.0.6

- updated `AzureEventHubsLoad` to use a SNAPSHOT compiled JAR in `/lib` to get latest changes. this will be changed back to Maven once version is officially released. Also exposed the expontential retry options to API.
- initial testing of a `.zip` reader/writer.

## 1.0.5

- **FIX** a longstanding defect in `TypingTranform` not correctly passing through values which are already of correct type.
- change the `_index` field added to `*Extract` from `monotonically_increasing_id()` to a `row_number(monotonically_increasing_id())` so that the index aligns to underlying files and is more intuitive to use.

## 1.0.4

- allow passing of same metadata schema to `JSONExtract` and `XMLExtract` to reduce cost of schema inference for large number of files.

## 1.0.3

- Expose numPartitions Optional Parameter for `*Extract`.

## 1.0.2

- add sql validation step to `SQLValidate` configuration parsing and ensure parameters are injected first (including `SQLTransform`) so the statements with parameters can be parsed.

## 1.0.1

- bump to Spark [2.3.1](https://spark.apache.org/releases/spark-release-2-3-1.html).

## 1.0.0

- initial release.
