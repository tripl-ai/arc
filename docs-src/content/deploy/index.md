---
title: Deploy
weight: 80
type: blog
---

Arc has been packaged as a [Docker](https://hub.docker.com/u/triplai) image to simplify deployment as a stateless process on cloud infrastructure. As there are multiple versions of Arc, Spark, Scala and Hadoop see the [https://hub.docker.com/u/triplai](https://hub.docker.com/u/triplai) for the relevant version.

## Running a Job

An example command to start a job is:

```json
docker run \
-e "ETL_CONF_ENV=production" \
-e "ETL_CONF_JOB_PATH=/opt/tutorial/basic/job/0" \
-it -p 4040:4040 {{% docker_image %}} \
bin/spark-submit \
--master local[*] \
--class ai.tripl.arc.ARC \
/opt/spark/jars/arc.jar \
--etl.config.uri=file:///opt/tutorial/basic/job/0/basic.json
```

This job executes the following job file which is included in the docker image:

```json
{"stages":
  [{
    "type": "SQLValidate",
    "name": "a simple stage which prints a message",
    "environments": [
      "production",
      "test"
    ],
    "inputURI": ${ETL_CONF_JOB_PATH}"/print_message.sql",
    "sqlParams": {
      "message0": "Hello",
      "message1": "World!"
    },
    "authentication": {},
    "params": {}
  }]
}
```

This example is included to demonstrate:

- `ETL_CONF_ENV` is a reserved environment variable which determines which stages to execute in the current mode. For each of the stages the job designer can specify an array of `stages` under which that stage will be executed (in the case above `production` and `test` are specified).<br><br>The purpose of this stage is so that it is possible to add or remove stages for execution modes like `test` or `integration` which are executed by a [CI/CD](https://en.wikipedia.org/wiki/CI/CD) tool prior to deployment and that you do not want to run in `production` mode - so maybe a comparison against a known 'good' test dataset could be executed in only `test` mode.

- `ETL_CONF_JOB_PATH` is an environment variable that is parsed and included by string interpolation when the job file is executed. So when then job starts Arc will attempt to resolve all environment variables set in the `basic.json` job file. In this case `"inputURI": ${ETL_CONF_JOB_PATH}"/print_message.sql",` becomes `"inputURI": "/opt/tutorial/basic/job/0/print_message.sql",` after resolution. This is included so that potentially different paths would be set for running in `test` vs `production` mode.

- In this sample job the spark master is `local[*]` indicating that this is a single instance 'cluster' where Arc relies on [vertical](https://en.wikipedia.org/wiki/Scalability#Horizontal_and_vertical_scaling) not [horizonal](https://en.wikipedia.org/wiki/Scalability#Horizontal_and_vertical_scaling) scaling. Depending on the constrains of the job (i.e. CPU vs disk IO) it is often better to execute with vertical scaling on cloud compute rather than pay the cost of network shuffling.

- `etl.config.uri` is a reserved JVM property which describes to Arc which job to execute. See below for all the properties that can be passed to Arc.

## Configuration Parameters

| Variable | Property | Description |
|----------|----------|-------------|
|ETL_CONF_ENABLE_STACKTRACE|etl.config.enableStackTrace|Whether to enable stacktraces in the event of exception which can be useful for debugging but is not very intuitive for many users. Boolean. Default `false`.|
|ETL_CONF_ENV_ID|etl.config.environment.id|An environment identifier to be added to all logging messages. Could be something like a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) which allows joining to logs produced by ephemeral compute started by something like [Terraform](https://www.terraform.io/).|
|ETL_CONF_ENV|etl.config.environment|The `environment` to run under.<br><br>E.g. if `ETL_CONF_ENV` is set to `production` then a stage with `"environments": ["production", "test"]` would be executed and one with `"environments": ["test"]` would not be executed.|
|ETL_CONF_IGNORE_ENVIRONMENTS|etl.config.ignoreEnvironments|Allows skipping the `environments` tests and execute all stages/plugins.|
|ETL_CONF_JOB_ID|etl.config.job.id|A job identifier added to all the logging messages.|
|ETL_CONF_JOB_NAME|etl.config.job.name|A job name added to all logging messages and Spark history server.|
|ETL_CONF_STORAGE_LEVEL|etl.config.storageLevel|The [StorageLevel](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.storage.StorageLevel$) used when persisting datasets. String. Default `MEMORY_AND_DISK_SER`.|
|ETL_CONF_STREAMING|etl.config.streaming|Run in [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) mode or not. Boolean. Default `false`.|
|ETL_CONF_TAGS|etl.config.tags|Custom key/value tags separated by space to add to all logging messages.<br><br>E.g. `ETL_CONF_TAGS=cost_center=123456 owner=jovyan`.|
|ETL_CONF_URI|etl.config.uri|The URI of the job file to execute.|

Additionally there are permissions arguments that can be used to retrieve the job file from cloud storage:

| Variable | Property | Description |
|----------|----------|-------------|
|ETL_CONF_ADL_OAUTH2_CLIENT_ID|etl.config.fs.adl.oauth2.client.id|The OAuth client identifier for connecting to Azure Data Lake.|
|ETL_CONF_ADL_OAUTH2_REFRESH_TOKEN|etl.config.fs.adl.oauth2.refresh.token|The OAuth refresh token for connecting to Azure Data Lake.|
|ETL_CONF_AZURE_ACCOUNT_KEY|etl.config.fs.azure.account.key|The account key for connecting to Azure Blob Storage.|
|ETL_CONF_AZURE_ACCOUNT_NAME|etl.config.fs.azure.account.name|The account name for connecting to Azure Blob Storage.|
|ETL_CONF_GOOGLE_CLOUD_AUTH_SERVICE_ACCOUNT_JSON_KEYFILE|etl.config.fs.google.cloud.auth.service.account.json.keyfile|The service account json keyfile path for connecting to Google Cloud Storage.|
|ETL_CONF_GOOGLE_CLOUD_PROJECT_ID|etl.config.fs.gs.project.id|The project identifier for connecting to Google Cloud Storage.|
|ETL_CONF_S3A_ACCESS_KEY|etl.config.fs.s3a.access.key|The access key for connecting to Amazon S3.|
|ETL_CONF_S3A_CONNECTION_SSL_ENABLED|etl.config.fs.s3a.connection.ssl.enabled|Whether to enable SSL connection to Amazon S3.|
|ETL_CONF_S3A_ENDPOINT|etl.config.fs.s3a.endpoint|The endpoint for connecting to Amazon S3.|
|ETL_CONF_S3A_SECRET_KEY|etl.config.fs.s3a.secret.key|The secret for connecting to Amazon S3.|

## Examples

### Streaming

This is an example of a streaming job [source](https://github.com/tripl-ai/arc/blob/master/tutorial/streaming/job/0/streaming.json). This job is intended to be executed after the integration test envornment has been started:

Start integration test environments:

```
docker-compose -f src/it/resources/docker-compose.yml up --build -d
```

Start the streaming job:

```bash
docker run \
--net "arc-integration" \
-e "ETL_CONF_ENV=test" \
-e "ETL_CONF_STREAMING=true" \
-e "ETL_CONF_ROWS_PER_SECOND=10" \
-it -p 4040:4040 {{% docker_image %}} \
bin/spark-submit \
--master local[*] \
--class ai.tripl.arc.ARC \
/opt/spark/jars/arc.jar \
--etl.config.uri=file:///opt/tutorial/streaming/job/0/streaming.json
```

## Spark and ulimit

On larger instances with many cores per machine it is possible to exceed the default (`1024`) max open files (`ulimit`). This should be verified on your instances if you are receiving `too many open files` type errors.