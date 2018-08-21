## Change Log

# 1.0.9

- added the ability to add column level `metadata`.
- added the `MetadataFilterTransform` stage - a stage which allows filtering of columns by their metadata.
- added the `PipelineExecute` stage - a stage which allows embeding other pipelines.
- added the `HTTPTransform` stage - a stage which calls an external API and adds result to Dataframe.

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

- bump to Spark 2.3.1.

# 1.0.0

- initial release.