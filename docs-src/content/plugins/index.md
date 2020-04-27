---
title: Plugins
weight: 85
type: blog
---

Arc has been designed so that it can be extended by simply building a JAR with logic that meets the [interface](#custom-plugins) specifications and placing it in the classpath. The rationalle for this is to allow teams to add custom functionality easily and not be reliant on a central team for development.

Arc can be exended in four ways by registering:

- [Dynamic Configuration Plugins](#dynamic-configuration-plugins) which allow users to inject custom configuration parameters which will be processed before resolving the job configuration file.
- [Lifecycle Plugins](#lifecycle-plugins) which allow users to extend the base Arc framework with pipeline lifecycle hooks.
- [Pipeline Stage Plugins](#pipeline-stage-plugins) which allow users to extend the base Arc framework with custom stages which allow the full use of the Spark [Scala API](https://spark.apache.org/docs/latest/api/scala/). All the Arc core stages are built by using this plugin interface.
- [User Defined Functions](#user-defined-functions) which extend the Spark SQL dialect.

## Included Plugins

### Lifecycle Plugins

#### ChaosMonkey
##### Since: 2.10.0 - Supports Streaming: False

The `ChaosMonkey` plugin is intended to be used for testing your orchestration design. It will randomly execute a strategy `after` each stage such as to throw an `exception` based on a `probability`.

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|name|String|true|{{< readfile file="/content/partials/fields/stageName.md" markdown="true" >}}|
|environments|Array[String]|true|{{< readfile file="/content/partials/fields/environments.md" markdown="true" >}}|
|strategy|String|true|The strategy to apply. Supported values: `exception`.<br><br>Default: `exception`.|
|probability|Double|true|The probability of this strategy being executed. Must be between `0.0` and `1.0`.|

### Examples

{{< readfile file="/resources/docs_resources_plugins/ChaosMonkeyComplete" highlight="json" >}}

### User Defined Functions

To help with common data tasks several additional functions have been added to Arc in addition to the inbuilt [Spark SQL Functions](https://spark.apache.org/docs/latest/api/sql/index.html).

#### get_json_double_array
##### Since: 1.0.9

{{< note title="Deprecated" >}}
Deprecated. Please use inbuilt Spark function [get_json_object](https://spark.apache.org/docs/latest/api/sql/index.html#get_json_object).
{{</note>}}

Similar to [get_json_object](https://spark.apache.org/docs/latest/api/sql/index.html#get_json_object) - but extracts a json `double` `array` from path.

```sql
SELECT get_json_double_array('[0.1, 1.1]', '$')
```

#### get_json_integer_array
##### Since: 1.0.9

{{< note title="Deprecated" >}}
Deprecated. Please use inbuilt Spark function [get_json_object](https://spark.apache.org/docs/latest/api/sql/index.html#get_json_object).
{{</note>}}

Similar to [get_json_object](https://spark.apache.org/docs/latest/api/sql/index.html#get_json_object) - but extracts a json `integer` `array` from path.

```sql
SELECT get_json_integer_array('[1, 2]', '$')
```

#### get_json_long_array
##### Since: 1.0.9

{{< note title="Deprecated" >}}
Deprecated. Please use inbuilt Spark function [get_json_object](https://spark.apache.org/docs/latest/api/sql/index.html#get_json_object).
{{</note>}}

Similar to [get_json_object](https://spark.apache.org/docs/latest/api/sql/index.html#get_json_object) - but extracts a json `long` `array` from path.

```sql
SELECT get_json_long_array('[2147483648, 2147483649]', '$')
```

#### get_uri
##### Since: 2.10.1

`get_uri` returns the contents of a URI as an `Array[Byte]`. If reading text this function can be wrapped with the inbuilt [decode](https://spark.apache.org/docs/latest/api/sql/index.html#decode) Spark SQL function to convert from `Array[Byte]` to `string` like: `DECODE(GET_URI('s3a://bucket/file.txt'), 'UTF-8')`. Prior to Arc 3.x this will not allow authentication to be modified from the standard inbuilt permissions (like `AmazonIAM`).

```sql
SELECT get_uri('s3a://bucket/file.txt') AS content
```

#### to_xml
##### Since: 2.10.0

`to_xml` returns a XML string with a given struct value.

```sql
SELECT
  to_xml(
    NAMED_STRUCT(
      'Document', NAMED_STRUCT(
          '_VALUE', NAMED_STRUCT(
            'child0', 0,
            'child1', NAMED_STRUCT(
              'nested0', 0,
              'nested1', 'nestedvalue'
            )
          ),
      '_attribute', 'attribute'
      )
    )
  ) AS xml
```

Produces a the XML string:

```xml
<Document attribute="attribute">
  <child0>0</child0>
  <child1>
    <nested0>0</nested0>
    <nested1>nestedvalue</nested1>
  </child1>
</Document>
```

#### struct_keys
##### Since: 2.10.0

`struct_keys` returns an array with the names of the keys in the struct.

```sql
SELECT
  STRUCT_KEYS(
    NAMED_STRUCT(
      'key0', 'value0',
      'key1', 'value1'
    )
  )
```


## Custom Plugins


### Resolution

Plugins are resolved dynamically at runtime and are resolved by name and version.

#### Examples

Assuming we wanted to execute a `KafkaExtract` [Pipeline Stage Plugin](#pipeline-stage-plugins):

{{< readfile file="/resources/docs_resources_plugins/KafkaExtractMin" highlight="json" >}}

Arc will attempt to resolve the plugin by first looking in all the `META-INF` directories of all included `JAR` files (https://github.com/tripl-ai/arc-kafka-pipeline-plugin/blob/master/src/main/resources/META-INF/services/ai.tripl.arc.plugins.PipelineStagePlugin) for classes that extend `PipelineStagePlugin` which the `KafkaExtract` plugin does:

```scala
class KafkaExtract extends PipelineStagePlugin {
```

Arc is then able to resolve the plugin by matching on `simpleName` - in this case `KafkaExtract` - and then call the `instantiate()` method to create an instance of the plugin which is executed by Arc at the appropriate time depending on plugin type.

To allow more specitivity you can use either the full package name and/or include the version:

```json
{
  "type": "ai.tripl.arc.extract.KafkaExtract",
  ...
```

```json
{
  "type": "KafkaExtract:1.0.0",
  ...
```


```json
{
  "type": "ai.tripl.arc.extract.KafkaExtract:1.0.0",
  ...
```


### Dynamic Configuration Plugins
##### Since: 1.3.0

{{<note title="Dynamic vs Deterministic Configuration">}}
Use of this functionality is discouraged as it goes against the [principles of Arc](/#principles) specifically around statelessness/deterministic behaviour but is inlcuded here for users who have not yet committed to a job orchestrator such as [Apache Airflow](https://airflow.apache.org/) and have dynamic configuration requirements.
{{</note>}}

The `Dynamic Configuration Plugin` plugin allow users to inject custom configuration parameters which will be processed before resolving the job configuration file. The plugin must return a Typesafe Config object (which is easily created from a `java.util.Map[String, Object]` which will be included in the job configuration resolution step.

#### Examples

For example a custom runtime configuration plugin could be used calculate a formatted list of dates to be used with an [Extract](../extract) stage to read only a subset of documents:

```scala
package ai.tripl.arc.plugins.config

import java.util
import java.sql.Date
import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.format.ResolverStyle
import scala.collection.JavaConverters._

import com.typesafe.config._

import org.apache.spark.sql.SparkSession

import ai.tripl.arc.util.log.logger.Logger
import ai.tripl.arc.api.API.ARCContext
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.DynamicConfigurationPlugin

class DeltaPeriodDynamicConfigurationPlugin extends DynamicConfigurationPlugin {

  val version = ai.tripl.arc.plugins.config.deltaperiod.BuildInfo.version

  def instantiate(index: Int, config: Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[StageError], Config] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "environments" :: "returnName" :: "lagDays" :: "leadDays" :: "formatter" :: "currentDate" :: Nil
    val returnName = getValue[String]("returnName")
    val lagDays = getValue[Int]("lagDays")
    val leadDays = getValue[Int]("leadDays")
    val formatter = getValue[String]("formatter") |> parseFormatter("formatter") _
    val currentDate = formatter match {
      case Right(formatter) => {
        if (c.hasPath("currentDate")) getValue[String]("currentDate") |> parseCurrentDate("currentDate", formatter) _ else Right(java.time.LocalDate.now)
      }
      case _ => Right(java.time.LocalDate.now)
    }
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (returnName, lagDays, leadDays, formatter, currentDate, invalidKeys) match {
      case (Right(returnName), Right(lagDays), Right(leadDays), Right(formatter), Right(currentDate), Right(invalidKeys)) =>

        val res = (lagDays * -1 to leadDays).map { v =>
          formatter.format(currentDate.plusDays(v))
        }.mkString(",")

        val values = new java.util.HashMap[String, Object]()
        values.put(returnName, res)

        Right(ConfigFactory.parseMap(values))
      case _ =>
        val allErrors: Errors = List(returnName, lagDays, leadDays, formatter, currentDate, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val err = StageError(index, this.getClass.getName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def parseFormatter(path: String)(formatter: String)(implicit c: Config): Either[Errors, DateTimeFormatter] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, DateTimeFormatter] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      Right(DateTimeFormatter.ofPattern(formatter).withResolverStyle(ResolverStyle.SMART))
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }

  def parseCurrentDate(path: String, formatter: DateTimeFormatter)(value: String)(implicit c: Config): Either[Errors, LocalDate] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, LocalDate] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      Right(LocalDate.parse(value, formatter))
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }
}

```

The plugin then needs to be registered in the `plugins.config` section of the job configuration and the full plugin name must be listed in your project's `/resources/META-INF/services/ai.tripl.arc.plugins.DynamicConfigurationPlugin` file. See https://github.com/tripl-ai/arc-deltaperiod-config-plugin for a full example.

Note that the resolution order of these plugins is in descending order in that if the the `ETL_CONF_DELTA_PERIOD` was declared in multiple plugins the value set by the plugin with the lower index in the `plugins.config` array will take precedence.

The `ETL_CONF_DELTA_PERIOD` variable is then available to be resolved in a standard configuration:

```json
{
  "plugins": {
    "config": [
      {
        "type": "ai.tripl.arc.plugins.config.DeltaPeriodDynamicConfigurationPlugin",
        "environments": [
          "production",
          "test"
        ],
        "returnName": "ETL_CONF_DELTA_PERIOD",
        "lagDays": "10",
        "leadDays": "1",
        "pattern": "yyyy-MM-dd"
      }
    ]
  },
  "stages": [
    {
      "type": "DelimitedExtract",
      "name": "load customer extract",
      "environments": [
        "production",
        "test"
      ],
      "inputURI": "hdfs://datalake/input/customer/customers_{"${ETL_CONF_DELTA_PERIOD}"}.csv",
      "outputView": "customer"
    }
  ]
}
```


### Lifecycle Plugins
##### Since: 1.3.0

Custom `Lifecycle Plugins` allow users to extend the base Arc framework with logic which is executed `before` or `after` each Arc stage (lifecycle hooks). These stages are useful for implementing things like dataset logging after each stage execution for debugging.

#### Examples

```scala
package ai.tripl.arc.plugins.lifecycle

import org.apache.spark.sql.{DataFrame, SparkSession}

import ai.tripl.arc.api.API._
import ai.tripl.arc.plugins.LifecyclePlugin
import ai.tripl.arc.util.Utils
import ai.tripl.arc.config.Error._

class DataFramePrinter extends LifecyclePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], LifecyclePluginInstance] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "environments" :: "numRows" :: "truncate" :: Nil
    val numRows = getValue[Int]("numRows", default = Some(20))
    val truncate = getValue[java.lang.Boolean]("truncate", default = Some(true))
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (numRows, truncate, invalidKeys) match {
      case (Right(numRows), Right(truncate), Right(invalidKeys)) =>
        Right(DataFramePrinterInstance(
          plugin=this,
          numRows=numRows,
          truncate=truncate
        ))
      case _ =>
        val allErrors: Errors = List(numRows, truncate, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val err = StageError(index, this.getClass.getName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class DataFramePrinterInstance(
    plugin: LifecyclePlugin,
    numRows: Int,
    truncate: Boolean
  ) extends LifecyclePluginInstance {

  override def before(stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext) {
    logger.trace()
      .field("event", "before")
      .field("stage", stage.name)
      .log()
  }

  override def after(result: Option[DataFrame], stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext) {
    logger.trace()
      .field("event", "after")
      .field("stage", stage.name)
      .log()

    result match {
      case Some(df) => df.show(numRows, truncate)
      case None =>
    }
  }
}
```

The plugin then needs to be registered by adding the full plugin name must be listed in your project's `/resources/META-INF/services/ai.tripl.arc.plugins.LifecyclePlugin` file.

To execute:

```json
{
  "plugins": {
    "lifecycle": [
      {
        "type": "ai.tripl.arc.plugins.lifecycle.DataFramePrinterLifecyclePlugin",
        "environments": [
          "production",
          "test"
        ],
        "params": {
          "numRows": "100",
          "truncate": "false",
        }
      }
    ]
  },
  "stages": [
    ...
  ]
}
```


### Pipeline Stage Plugins
##### Since: 1.3.0

Custom `Pipeline Stage Plugins` allow users to extend the base Arc framework with custom stages which allow the full use of the Spark [Scala API](https://spark.apache.org/docs/latest/api/scala/). This means that private business logic or code which relies on libraries not included in the base Arc framework can be used - however it is strongly advised to use the inbuilt SQL stages where possible. If stages are general purpose enough for use outside your organisation consider contributing them to [ai.tripl](https://github.com/tripl-ai) so that others can benefit.

When writing plugins and you find Spark throwing `NotSerializableException` errors like:

```scala
Job aborted due to stage failure: Task not serializable: java.io.NotSerializableException: scala.collection.convert.Wrappers$MapWrapper
```

Ensure that any stage with a `mapPartitions` or `map` DataFrame does not require the `PipelineStage` instance to be passed into the `map` function. So instead of doing something like:

```scala
val transformedDF = try {
  df.mapPartitions[TransformedRow] { partition: Iterator[Row] =>
    val uri = stage.uri.toString
```

Declare the variables outside the map function so that `stage` does not have to be serialised and sent to all the executors (which fails if any of the `PipelineStage` contents are not serializable):

```scala
val stageUri = stage.uri

val transformedDF = try {
  df.mapPartitions[TransformedRow] { partition: Iterator[Row] =>
    val uri = stageUri.toString
```

#### Examples

```scala
class ConsoleLoad extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputMode" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputMode = getValue[String]("outputMode", default = Some("Append"), validValues = "Append" :: "Complete" :: "Update" :: Nil) |> parseOutputModeType("outputMode") _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, outputMode, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputMode), Right(invalidKeys)) =>
        val stage = ConsoleLoadStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          outputMode=outputMode,
          params=params
        )

        stage.stageDetail.put("inputView", stage.inputView)
        stage.stageDetail.put("outputMode", stage.outputMode.sparkString)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputMode, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class ConsoleLoadStage(
    plugin: ConsoleLoad,
    name: String,
    description: Option[String],
    inputView: String,
    outputMode: OutputModeType,
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    ConsoleLoadStage.execute(this)
  }
}


object ConsoleLoadStage {

  def execute(stage: ConsoleLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val df = spark.table(stage.inputView)

    if (!df.isStreaming) {
      throw new Exception("ConsoleLoad can only be executed in streaming mode.") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    df.writeStream
        .format("console")
        .outputMode(stage.outputMode.sparkString)
        .start

    Option(df)
  }
}
```

The plugin then needs to be registered by adding the full plugin name must be listed in your project’s `/resources/META-INF/services/ai.tripl.arc.plugins.PipelineStagePlugin` file.

To execute:

```json
{
  "stages": [
    {
      "type": "ConsoleLoad",
      "name": "load streaming data to console for testing",
      "environments": [
        "test"
      ],
      "inputView": "calculated_dataset",
      "outputMode": "Complete"
    }
  ]
}
```

### User Defined Functions
##### Since: 1.3.0

{{<note title="User Defined Functions vs Spark SQL Functions">}}
The inbuilt [Spark SQL Functions](https://spark.apache.org/docs/latest/api/sql/index.html) are heavily optimised by the internal Spark code to a level which custom User Defined Functions cannot be (byte code) - so where possible it is better to use the inbuilt functions.
{{</note>}}

`User Defined Functions` allow users to extend the [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) dialect.

Arc already includes [some addtional functions](partials/#user-defined-functions) which are not included in the base [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) dialect so any useful generic functions can be included in the [Arc repository](https://github.com/tripl-ai/arc) so that others can benefit.

#### Examples

Write the code to define the custom `User Defined Function`:

```scala
package ai.tripl.arc.plugins
import java.util

import org.apache.spark.sql.SparkSession
import ai.tripl.arc.api.API.ARCContext

import ai.tripl.arc.util.log.logger.Logger

class TestUDFPlugin extends UDFPlugin {

  val version = "0.0.1"

  // one udf plugin can register multiple user defined functions
  override def register()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext) = {

    // register the functions so they can be accessed via Spark SQL
    spark.sqlContext.udf.register("add_ten", TestUDFPlugin.addTen _ )           // SELECT add_ten(1) AS one_plus_ten
    spark.sqlContext.udf.register("add_twenty", TestUDFPlugin.addTwenty _ )     // SELECT add_twenty(1) AS one_plus_twenty

  }
}

object TestUDFPlugin {
  // add 10 to an incoming integer - DO NOT DO THIS IN PRODUCTION INSTEAD USE SPARK SQL DIRECTLY
  def addTen(input: Int): Int = {
    input + 10
  }

  // add 20 to an incoming integer  - DO NOT DO THIS IN PRODUCTION INSTEAD USE SPARK SQL DIRECTLY
  def addTwenty(input: Int): Int = {
    input + 20
  }
}
```

The plugin then needs to be registered by adding the full plugin name must be listed in your project's `/resources/META-INF/services/ai.tripl.arc.plugins.UDFPlugin` file and would be executed like:

```sql
SELECT age, add_ten(age) FROM customer
```

