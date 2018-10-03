---
title: Extend
weight: 98
type: blog
---

Arc can be exended in three ways by registering:

- [Dynamic Configuration Plugins](#dynamic-configuration-plugins).
- [Pipeline Stage Plugins](#pipeline-stage-plugins).
- [User Defined Functions](#user-defined-functions) which extend the Spark SQL dialect.

## Dynamic Configuration Plugins
##### Since: 1.3.0

{{<note title="Dynamic Configuration vs Determinism">}}
Use of this functionality is discouraged as it goes against the [principles of Arc](/#principles) specifically around statelessness/deterministic behaviour but is inlcuded here for users who have not yet committed to a job orchestrator such as [Apache Airflow](https://airflow.apache.org/) and have dynamic configuration requirements.
{{</note>}}

The `Dynamic Configuration Plugin` plugin allow users to inject custom configuration parameters which will be processed before resolving the job configuration file. The plugin must return a `Map[String, Object]` which will be included in the job configuration resolution step.

### Examples

For example a custom runtime configuration plugin could be used calculate a business specific configuration for a business with a processing day on the last `THURSDAY` of each month:

```scala
package au.com.myfakebusiness.plugins

import java.time.{DayOfWeek, LocalDate}
import java.time.temporal.TemporalAdjusters.lastInMonth
import java.util

import au.com.agl.arc.plugins
import au.com.agl.arc.util.log.logger.Logger

class MyFakeBusinessLastProcessingDayPlugin extends ConfigPlugin {
  override def values()(implicit logger: Logger): util.Map[String, Object] = {

    val lastProcessingDay = LocalDate.now.`with`(lastInMonth(DayOfWeek.THURSDAY))

    val values = new java.util.HashMap[String, Object]()
    values.put("ETL_CONF_LAST_PROCESSING_DAY", lastProcessingDay.toString)

    values
  }
}
```

The plugin then needs to be registered in the `plugins.config` section of the job configuration and the full plugin name must be listed in your project's `/resources/META-INF/services/au.com.agl.arc.plugins.DynamicConfigurationPlugin` file. See [this example](https://github.com/AGLEnergy/arc/blob/master/src/test/resources/META-INF/services/au.com.agl.arc.plugins.DynamicConfigurationPlugin). 

Note that the resolution order of these plugins is in descending order in that if the the `ETL_CONF_LAST_PROCESSING_DAY` was declared in multiple plugins the value set by the plugin with the lower index in the `plugins.config` array will take precedence.

The `ETL_CONF_LAST_PROCESSING_DAY` variable is then available to be resolved in a standard configuration:

```json
{
  "plugins": {
    "config": [
      "au.com.myfakebusiness.plugins.MyFakeBusinessLastProcessingDayPlugin"
    ]
  },
  "stages": [
    {
      "type": "SQLTransform",
      "name": "run the end of month report",
      "environments": ["production", "test"],
      "inputURI": "hdfs://datalake/sql/0.0.1/endOfMonthReport.sql",
      "outputView": "endOfMonthReport",            
      "persist": false,
      "authentication": {
          ...
      },    
      "sqlParams": {
          "processing_date": ${ETL_CONF_LAST_PROCESSING_DAY}
      },    
      "params": {
      }
    }
  ]
}

```

## Pipeline Stage Plugins
##### Since: 1.3.0

Custom `Pipeline Stage Plugins` allow users to extend the base Arc framework with custom stages which allow the full use of the Spark [Scala API](https://spark.apache.org/docs/latest/api/scala/). This means that private business logic or code which relies on libraries not included in the base Arc framework can be used - however it is strongly advised to use the inbuilt SQL stages where possible. These stages can use the `params` map to be able to pass configuration parameters.

If stages are general purpose enough for use outside your organisation consider creating a pull request against the main [Arc repository](https://github.com/aglenergy/arc) so that others can benefit.

### Examples

```scala
package au.com.myfakebusiness.plugins

import au.com.agl.arc.plugins
import au.com.agl.arc.util.log.logger.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

class MyFakeBusinessAddCopyrightStage extends PipelineStagePlugin {
  override def execute(name: String, params: Map[String, String])(implicit spark: SparkSession, logger: Logger): Option[DataFrame] = {
    val startTime = System.currentTimeMillis() 

    val inputView = params.get("inputView").getOrElse("")
    val outputView = params.get("outputView").getOrElse("")
    val copyrightStatement = params.get("copyrightStatement").getOrElse("")

    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("name", name)
    stageDetail.put("inputView", inputView)  
    stageDetail.put("outputView", outputView)  
    stageDetail.put("copyrightStatement", copyrightStatement)
    
    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()


    // get existing dataframe
    val df = spark.table(inputView)

    // add copyright statement
    val enrichedDF = df.withColumn("copyright", lit(copyrightStatement))

    // register output view
    enrichedDF.createOrReplaceTempView(outputView)

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log() 

    Option(enrichedDF)    
  }
}
```

To execute:

```json
{
  "stages": [
    {
      "type": "au.com.mybusiness.plugins.MyFakeBusinessAddCopyrightStage",
      "name": "add copyright to each row",
      "environments": [
        "production",
        "test"
      ],
      "params": {
        "inputView": "calculated_dataset",
        "copyrightStatement": "copyright 2018 MyBusiness.com.au",
        "outputView": "final_dataset"
      }
    }
  ]
}
```

## User Defined Functions
##### Since: 1.3.0

{{<note title="User Defined Functions vs Spark SQL Functions">}}
The inbuilt [Spark SQL Functions](https://spark.apache.org/docs/latest/api/sql/index.html) are heavily optimised by the internal Spark code to a level which custom User Defined Functions cannot be (byte code) - so where possible it is better to use the inbuilt functions.
{{</note>}}

`User Defined Functions` allow users to extend the [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) dialect. 

Arc already includes [some addtional functions](partials/#user-defined-functions) which are not included in the base [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) dialect so any useful generic functions can be included in the [Arc repository](https://github.com/aglenergy/arc) so that others can benefit.

### Examples

Write the code to define the custom `User Defined Function`:

```scala
package au.com.agl.arc.plugins
import java.util

import org.apache.spark.sql.SQLContext

import au.com.agl.arc.util.log.logger.Logger

class UDFPluginTest extends UDFPlugin {
  // one udf plugin can register multiple user defined functions
  override def register(sqlContext: SQLContext)(implicit logger: au.com.agl.arc.util.log.logger.Logger): Seq[String] = {
    
    // register the functions so they can be accessed via Spark SQL
    // SELECT add_ten(1) AS one_plus_ten
    sqlContext.udf.register("add_ten", UDFPluginTest.addTen _ )
    
    // return the list of udf names that were registered for logging
    Seq("add_ten")
  }
}

object UDFPluginTest {
  // add 10 to an incoming integer - DO NOT DO THIS IN PRODUCTION INSTEAD USE SPARK SQL DIRECTLY
  def addTen(input: Int): Int = {
    input + 10
  }
}
```

The plugin then needs to be registered by adding the full plugin name must be listed in your project's `/resources/META-INF/services/au.com.agl.arc.plugins.UDFPlugin` file.