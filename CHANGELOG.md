## Change Log

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