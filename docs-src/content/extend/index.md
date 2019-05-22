---
title: Extend
weight: 98
type: blog
---

Arc can be exended in four ways by registering:

- [Dynamic Configuration Plugins](#dynamic-configuration-plugins).
- [Lifecycle Plugins](#lifecycle-plugins).
- [Pipeline Stage Plugins](#pipeline-stage-plugins).
- [User Defined Functions](#user-defined-functions) which extend the Spark SQL dialect.

## Dynamic Configuration Plugins
##### Since: 1.3.0

{{<note title="Dynamic vs Deterministic Configuration">}}
Use of this functionality is discouraged as it goes against the [principles of Arc](/#principles) specifically around statelessness/deterministic behaviour but is inlcuded here for users who have not yet committed to a job orchestrator such as [Apache Airflow](https://airflow.apache.org/) and have dynamic configuration requirements.
{{</note>}}

The `Dynamic Configuration Plugin` plugin allow users to inject custom configuration parameters which will be processed before resolving the job configuration file. The plugin must return a Java `Map[String, Object]` which will be included in the job configuration resolution step.

### Examples

For example a custom runtime configuration plugin could be used calculate a formatted list of dates to be used with an [Extract](../extract) stage to read only a subset of documents:

```scala
package au.com.agl.arc.plugins.config

import scala.collection.JavaConverters._

import au.com.agl.arc.plugins._
import au.com.agl.arc.util.log.logger.Logger

import java.sql.Date
import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.format.ResolverStyle

class DeltaPeriodDynamicConfigurationPlugin extends DynamicConfigurationPlugin {

  override def values(params: Map[String, String])(implicit logger: au.com.agl.arc.util.log.logger.Logger): java.util.Map[String, Object] = {
    val startTime = System.currentTimeMillis() 

    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", "DeltaPeriodDynamicConfigurationPlugin")
    stageDetail.put("pluginVersion", BuildInfo.version)
    stageDetail.put("params", params.asJava)

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()   

    // input validation
    val returnName = params.get("returnName") match {
        case Some(returnName) => returnName.trim
        case None => throw new Exception("required parameter 'returnName' not found.")
    }

    val lagDays = params.get("lagDays") match {
        case Some(lagDays) => {
            try {
            lagDays.toInt * -1
            } catch {
                case e: Exception => throw new Exception(s"cannot convert lagDays ('${lagDays}') to integer.")
            }
        }
        case None => throw new Exception("required parameter 'lagDays' not found.")
    }

    val leadDays = params.get("leadDays") match {
        case Some(leadDays) => {
            try {
            leadDays.toInt
            } catch {
                case e: Exception => throw new Exception(s"cannot convert leadDays ('${leadDays}') to integer.")
            }
        }
        case None => throw new Exception("required parameter 'leadDays' not found.")
    }

    val formatter = params.get("pattern") match {
        case Some(pattern) => {
            try {
                DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.SMART)
            } catch {
                case e: Exception => throw new Exception(s"cannot parse pattern ('${pattern}').")
            }
        }
        case None => throw new Exception("required parameter 'pattern' not found.")
    }

    val currentDate = params.get("currentDate") match {
        case Some(currentDate) => {
            try {
                LocalDate.parse(currentDate, formatter)
            } catch {
                case e: Exception => throw new Exception(s"""cannot parse currentDate ('${currentDate}') with formatter '${params.get("pattern").getOrElse("")}'.""")
            }
        }
        case None => java.time.LocalDate.now
    }


    // calculate the range 
    // produces a value that looks like "2018-12-31,2019-01-01,2019-01-02,2019-01-03,2019-01-04,2019-01-05,2019-01-06"
    val res = (lagDays to leadDays).map { v =>
      formatter.format(currentDate.plusDays(v))
    }.mkString(",")

    // set the return value
    val values = new java.util.HashMap[String, Object]()
    values.put(returnName, res)

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()  

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
      {
        "type": "au.com.agl.arc.plugins.config.DeltaPeriodDynamicConfigurationPlugin",
        "environments": [
          "production",
          "test"
        ],
        "params": {
          "returnName": "ETL_CONF_DELTA_PERIOD",
          "lagDays": "10",
          "leadDays": "1",
          "pattern": "yyyy-MM-dd"
        }
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

## Lifecycle Plugins
##### Since: 1.3.0

Custom `Lifecycle Plugins` allow users to extend the base Arc framework with logic which is executed `before` or `after` each Arc stage. These stages are useful for implementing things like dataset logging after each stage execution for debugging.

### Examples

```scala
package au.com.agl.arc.plugins.lifecycle

import java.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.Utils
import au.com.agl.arc.util.log.logger.Logger

class DataframePrinterLifecyclePlugin extends LifecyclePlugin {

  var params = Map[String, String]()

  override def setParams(p: Map[String, String]) {
    params = p
  }

  override def before(stage: PipelineStage)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger) {
    logger.trace()        
      .field("event", "before")
      .field("stage", stage.name)
      .field("stageType", stage.getType)
      .log()  
  }

  override def after(stage: PipelineStage, result: Option[DataFrame], isLast: Boolean)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger) {
    logger.trace()        
      .field("event", "after")
      .field("stage", stage.name)
      .field("stageType", stage.getType)
      .field("isLast", java.lang.Boolean.valueOf(isLast))
      .log() 

    result match {
      case Some(df) => {
        val numRows = params.get("numRows") match {
          case Some(n) => n.toInt
          case None => 20
        }

        val truncate = params.get("truncate") match {
          case Some(t) => t.toBoolean
          case None => true
        }  

        df.show(numRows, truncate)
      }
      case None =>
    }
  }

}
```

The plugin then needs to be registered by adding the full plugin name must be listed in your project’s `/resources/META-INF/services/au.com.agl.arc.plugins.LifecyclePlugin` file.

To execute:

```json
{
  "plugins": {
    "lifecycle": [
      {
        "type": "au.com.agl.arc.plugins.lifecycle.DataframePrinterLifecyclePlugin",
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

The plugin then needs to be registered by adding the full plugin name must be listed in your project’s `/resources/META-INF/services/au.com.agl.arc.plugins.PipelineStagePlugin` file.

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