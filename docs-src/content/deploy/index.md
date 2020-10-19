---
title: Deploy
weight: 80
type: blog
---

Arc has been packaged as a [Docker](https://hub.docker.com/u/triplai) image to simplify deployment as a stateless process on cloud infrastructure. As there are multiple versions of Arc, Spark, Scala and Hadoop see the [https://hub.docker.com/u/triplai](https://hub.docker.com/u/triplai) for the relevant version. The Arc container is built using the offical Spark Kubernetes images so running locally requires overriding the Docker `entrypoint`.

The [deploy](https://github.com/tripl-ai/deploy) repository has examples of how to run Arc jobs on common cloud environments.

## Arc Local

An example command to start a job from the [Arc Starter](https://github.com/tripl-ai/arc-starter) base directory:

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

This example is included to demonstrate:

- `ETL_CONF_ENV` is a reserved environment variable which determines which stages to execute in the current mode. For each of the stages the job designer can specify an array of `environments` under which that stage will be executed (in the case above `production` and `test` are specified).<br><br>The purpose of this stage is so that it is possible to add or remove stages for execution modes like `test` or `integration` which are executed by a [CI/CD](https://en.wikipedia.org/wiki/CI/CD) tool prior to deployment and that you do not want to run in `production` mode - so maybe a comparison against a known 'good' test dataset could be executed in only `test` mode.

- In this sample job the spark master is `local[*]` indicating that this is a single instance 'cluster' where Arc relies on [vertical](https://en.wikipedia.org/wiki/Scalability#Horizontal_and_vertical_scaling) not [horizonal](https://en.wikipedia.org/wiki/Scalability#Horizontal_and_vertical_scaling) scaling. Depending on the constrains of the job (i.e. CPU vs disk IO) it is often better to execute with vertical scaling on cloud compute rather than pay the cost of network shuffling.

- `etl.config.uri` is a reserved JVM property which describes to Arc which job to execute. See below for all the properties that can be passed to Arc.

## Arc on Kubernetes

Arc is built using the offical [Spark Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes) image [build process](https://spark.apache.org/docs/latest/running-on-kubernetes.html#docker-images) which allows Arc to be easily deployed to a Kubernetes cluster.

```bash
bin/spark-submit \
--master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
--deploy-mode cluster \
--name arc \
--class ai.tripl.arc.ARC \
--conf spark.executor.instances=1 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.container.image={{% docker_image %}}  \
--conf spark.kubernetes.driverEnv.ETL_CONF_ENV=production \
--conf spark.kubernetes.driverEnv.ETL_CONF_DATA_URL=s3a://nyc-tlc/trip*data \
--conf spark.kubernetes.driverEnv.ETL_CONF_JOB_URL=https://raw.githubusercontent.com/tripl-ai/arc-starter/master/examples/kubernetes \
local:///opt/spark/jars/arc.jar \
--etl.config.uri=https://raw.githubusercontent.com/tripl-ai/arc-starter/master/examples/kubernetes/nyctaxi.ipynb
```

## Configuration Parameters

| Environment Variable | Property | Description |
|----------|----------|-------------|
|ETL_CONF_ENABLE_STACKTRACE|etl.config.enableStackTrace|Whether to enable stacktraces in the event of exception which can be useful for debugging but is not very intuitive for many users. Boolean. Default `false`.|
|ETL_CONF_ENV|etl.config.environment|The `environment` to run under.<br><br>E.g. if `ETL_CONF_ENV` is set to `production` then a stage with `"environments": ["production", "test"]` would be executed and one with `"environments": ["test"]` would not be executed.|
|ETL_CONF_IGNORE_ENVIRONMENTS|etl.config.ignoreEnvironments|Allows skipping the `environments` tests and execute all stages/plugins.|
|ETL_CONF_JOB_ID|etl.config.job.id|A job identifier added to all the logging messages.|
|ETL_CONF_JOB_NAME|etl.config.job.name|A job name added to all logging messages and Spark history server.|
|ETL_CONF_LINT_ONLY|etl.config.lintOnly|Verify the job file and exit with `success`/`failure`.|
|ETL_CONF_STORAGE_LEVEL|etl.config.storageLevel|The [StorageLevel](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.storage.StorageLevel$) used when persisting datasets. String. Default `MEMORY_AND_DISK_SER`.|
|ETL_CONF_STREAMING|etl.config.streaming|Run in [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) mode or not. Boolean. Default `false`.|
|ETL_CONF_TAGS|etl.config.tags|Custom key/value tags separated by space to add to all logging messages.<br><br>E.g. `ETL_CONF_TAGS=cost_center=123456 owner=jovyan`.|
|ETL_CONF_URI|etl.config.uri|The URI of the job file to execute.|

## Policy Parameters

| Environment Variable | Property | Description |
|----------|----------|-------------|
|ETL_POLICY_INLINE_SCHEMA|etl.policy.inline.schema|Whether to support inline schemas (such as the `schema` attribute in `TypingTransform`) as opposed to force reading from an external file. Boolean. Default `true`.|
|ETL_POLICY_INLINE_SQL|etl.policy.inline.sql|Whether to support inline SQL (such as the `sql` attribute in `SQLTransform`) as opposed to force reading from an external file. Boolean. Default `true`.|
|ETL_POLICY_IPYNB|etl.policy.ipynb|Whether to support submission of IPython Notebook (.ipynb) files as opposed to Arc HOCON format only. Boolean. Default `true`.|
|ETL_POLICY_DROP_UNSUPPORTED|etl.policy.drop.unsupported|Whether to enable automatic dropping of unsupported types when performing `*Load` stages (e.g. `ParquetLoad` cannot support `NullType` and would be dropped if enabled).<br><br>If `NullType` columns have been created due to a SQL query (like `SELECT NULL AS fieldname`) it is sometimes possible to correctly type the column by `CAST`ing the column like `SELECT CAST(NULL AS INTEGER) AS fieldname` which will treat the column as an `IntegerType` containing only `NULL` values.<br><br>Default `false`.|


## Authentication Parameters

Permissions arguments can be used to retrieve the job file from cloud storage:

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
|ETL_CONF_S3A_ANONYMOUS|etl.config.fs.s3a.anonymous|Whether to connect to Amazon S3 in anonymous mode. e.g. `ETL_CONF_S3A_ANONYMOUS=true`.|
|ETL_CONF_S3A_ENCRYPTION_ALGORITHM|etl.config.fs.s3a.encryption.algorithm|The bucket encrpytion algorithm: `SSE-S3`, `SSE-KMS`, `SSE-C`.|
|ETL_CONF_S3A_KMS_ARN|The Key Management Service Amazon Resource Name when using `SSE-KMS` encryptionAlgorithm e.g. `arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab`.|
|ETL_CONF_S3A_CUSTOM_KEY|etl.config.fs.s3a.custom.key|The key to use when using Customer-Provided Encryption Keys (`SSE-C`).|

## Dynamic Variables

Sometimes it is useful to be able to utilise runtime only varaibles in an Arc job (aka. lazy evaluation), for example, dynamically calculating a partition to be read.

By default all stages have an implicit `resolution` key defaulting to `strict` which will try to resolve all parameters at the start of the job. By setting `resolution` to `lazy` it is possible to defer the resolution of the variables until execution time for that stage.

### Examples

This example calculates a list of distinct dates from the `new_transactions` dataset (like `CAST('2020-01-13' AS DATE),CAST('2020-01-14' AS DATE)`) and returns it as a variable named `ETL_CONF_DYNAMIC_PUSHDOWN` which is then used to read a subset of the `transactions` dataset. This pattern was used succesfully to force a certain behavior in the Spark SQL optimizer (predicate pushdown). Without the `resolution` equal to `lazy` the job would fail as the `${ETL_CONF_DYNAMIC_PUSHDOWN}` parameter would not be present at the beginning of the job.

{{< readfile file="/resources/docs_resources/DeployDynamicConfiguration" highlight="json" >}}


## Environments

The `Environments` list specifies a list of environments under which the stage will be executed. The environments list must contain the value in the `ETL_CONF_ENV` environment variable or `etl.config.environment` `spark-submit` argument for the stage to be executed.

### Examples

If a stage is to be executed in both production and testing and the `ETL_CONF_ENV` environment variable is set to `production` or `test` then the `DelimitedExtract` stage defined here will be executed. If the `ETL_CONF_ENV` environment variable was set to something else like `user_acceptance_testing` then this stage will not be executed and a warning message will be logged.

```json
{
    "type": "DelimitedExtract",
    ...
    "environments": ["production", "test"],
    ...
}
```

A practical use case of this is to execute additional stages in testing which would prevent the job from being automatically deployed to production via [Continuous Delivery](https://en.wikipedia.org/wiki/Continuous_delivery) if it fails:

```json
{
    "type": "ParquetExtract",
    "name": "load the manually verified known good set of data from testing",
    "environments": ["test"],
    "outputView": "known_correct_dataset",
    ...
},
{
    "type": "EqualityValidate",
    "name": "ensure the business logic produces the same result as the known good set of data from testing",
    "environments": ["test"],
    "leftView": "newly_caluclated_dataset",
    "rightView": "known_correct_dataset",
    ...
}
```

## Spark and ulimit

On larger instances with many cores per machine it is possible to exceed the default (`1024`) max open files (`ulimit`). This should be verified on your instances if you are receiving `too many open files` type errors.