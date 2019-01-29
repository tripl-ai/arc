package au.com.agl.arc

import java.net.URI
import java.util.UUID
import java.util.Properties

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 
import au.com.agl.arc.util._

class KafkaCommitExecuteSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val inputView0 = "inputView0"
  val inputView1 = "inputView1"
  val outputView = "outputView"
  val bootstrapServers = "localhost:29092"
  val timeout = 3000L

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    session = spark
    import spark.implicits._  
  }

  after {
    session.stop()
  }

  test("KafkaCommitExecute") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val dataset0 = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON
    dataset0.createOrReplaceTempView(inputView0)
    load.KafkaLoad.load(
      KafkaLoad(
        name="df", 
        inputView=inputView0, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None, 
        batchSize=16384, 
        retries=0, 
        params=Map.empty
      )
    )

    // read should have no offset saved as using uuid group id so get all 100 records
    val extractDataset0 = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000, 
        timeout=timeout, 
        autoCommit=false, 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty
      )
    ).get

    var expected = dataset0
    var actual = extractDataset0.select($"value").as[String]
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)

    // read should have no offset saved (autoCommit=false) so get all 100 records
    val extractDataset1 = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000, 
        timeout=timeout, 
        autoCommit=false, 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty
      )
    ).get

    expected = dataset0
    actual = extractDataset1.select($"value").as[String]
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)    

    // execute the update
    au.com.agl.arc.execute.KafkaCommitExecute.execute(
      KafkaCommitExecute(
        name="df", 
        inputView=outputView, 
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        params=Map.empty
      )
    ) 

    // read should now have offset saved so as no new records exist in kafka should return 0 records
    val extractDataset2 = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000, 
        timeout=timeout, 
        autoCommit=false, 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty
      )
    ).get
    actual = extractDataset2.select($"value").as[String]
    assert(actual.count === 0)

    // insert 200 records
    val dataset1 = spark.sqlContext.range(0, 200)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON
    dataset1.createOrReplaceTempView(inputView1)
    load.KafkaLoad.load(
      KafkaLoad(
        name="df", 
        inputView=inputView1, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None, 
        batchSize=16384, 
        retries=0, 
        params=Map.empty
      )
    ) 

    // read should now have offset saved so should only retieve records from second insert (200 records)
    val extractDataset3 = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000, 
        timeout=timeout, 
        autoCommit=false, 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty
      )
    ).get

    expected = dataset1
    actual = extractDataset3.select($"value").as[String]
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }      
}
