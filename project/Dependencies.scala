import sbt._

object Dependencies {
  // Versions
  lazy val sparkVersion = "2.4.3"
  lazy val scalaTestVersion = "3.0.7"

  // Testing
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "it,test"
  val scalactic = "org.scalactic" %% "scalactic" % scalaTestVersion % "it,test"
  val jetty = "org.mortbay.jetty" % "jetty" % "6.1.26" % "it,test"

  // Spark
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided" 
  val sparkMl = "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"

  // Spark XML
  val sparkXML = "com.databricks" %% "spark-xml" % "0.5.0" intransitive()

  // Spark AVRO
  val sparkAvro = "org.apache.spark" %% "spark-avro" % sparkVersion 

  // blob storage
  val hadoopAWS = "org.apache.hadoop" % "hadoop-aws" % "2.7.7" intransitive()
  val awsJavaSDK = "com.amazonaws" % "aws-java-sdk" % "1.7.4" intransitive()
  val hadoopAzure = "org.apache.hadoop" % "hadoop-azure" % "2.7.3" intransitive()
  val azureStorage = "com.microsoft.azure" % "azure-storage" % "3.1.0" intransitive()

  // jdbc drivers
  val sqlServerJDBC = "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.1.jre8" intransitive()
  val postgresJDBC = "org.postgresql" % "postgresql" % "42.2.5" intransitive()
  val presto = "com.facebook.presto" % "presto-jdbc" % "0.209" intransitive()
  val mysql = "mysql" % "mysql-connector-java" % "5.1.47" intransitive()

  // config reader
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1" intransitive()

  // scala graph
  val scala_graph_core = "org.scala-graph" %% "graph-core" % "1.11.5" intransitive()
  val scala_graph_dot = "org.scala-graph" %% "graph-dot" % "1.11.5" intransitive()
  val scala_graph_json = "org.scala-graph" %% "graph-json" % "1.11.0" intransitive()

  // Project
  val etlDeps = Seq(
    sparkCore,
    sparkSql,
    sparkHive,
    sparkMl,
    scalaTest,
    jetty,
    hadoopAWS,
    awsJavaSDK,
    hadoopAzure,
    azureStorage,   
    sqlServerJDBC,
    postgresJDBC,
    mysql,
    presto,
    typesafeConfig,
    scala_graph_core,
    scala_graph_dot,
    sparkXML,
    sparkAvro
  )
}