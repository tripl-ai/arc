import sbt._

object Dependencies {
  // Versions
  lazy val sparkVersion = "2.4.0"
  lazy val scalaTestVersion = "3.0.1"

  // Testing
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "it,test"
  val scalactic = "org.scalactic" %% "scalactic" % scalaTestVersion % "it,test"

  // Spark
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  val sparkMl = "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"

  // Spark XML
  val sparkXML = "com.databricks" %% "spark-xml" % "0.4.1" % "provided"

  // Spark AVRO
  val sparkAvro = "org.apache.spark" %% "spark-avro" % "2.4.0" % "provided"

  // Amazon S3
  val hadoopAWS = "org.apache.hadoop" % "hadoop-aws" % "2.7.3" % "provided"
  val awsJavaSDK = "com.amazonaws" % "aws-java-sdk" % "1.7.4" % "provided"

  // Azure Blob
  val hadoopAzure = "org.apache.hadoop" % "hadoop-azure" % "2.7.3" % "provided"
  val azureStorage = "com.microsoft.azure" % "azure-storage" % "3.1.0" % "provided"

  // Azure EventHubs
  val azureEventHub = "com.microsoft.azure" % "azure-eventhubs" % "1.2.0" % "provided"
  val qpid = "org.apache.qpid" % "proton-j" % "0.29.0" % "provided"

  // Azure AD
  val azureAD = "com.microsoft.azure" % "adal4j" % "1.2.0" % "provided"
  val azureKeyVault = "com.microsoft.azure" % "azure-keyvault" % "1.0.0" % "provided"

  // SQL Server
  val sqlServerJDBC = "com.microsoft.sqlserver" % "mssql-jdbc" % "6.4.0.jre8" % "provided"
  val postgresJDBC = "org.postgresql" % "postgresql" % "42.2.2" % "provided"

  // geospark
  val geospark = "org.datasyslab" % "geospark" % "1.1.1"  % "provided"
  val geosparkSQL = "org.datasyslab" % "geospark-sql_2.3" % "1.1.1" % "provided"

  // cli arg parsing
  val scallop = "org.rogach" %% "scallop" % "2.1.1"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1"

  // scala graph
  val scala_graph_core = "org.scala-graph" %% "graph-core" % "1.11.5"
  val scala_graph_dot = "org.scala-graph" %% "graph-dot" % "1.11.5"
  val scala_graph_json = "org.scala-graph" %% "graph-json" % "1.11.0"

  // hikari connection pool
  // val hikaricp = "com.zaxxer" % "HikariCP" % "3.2.0"

  // Project
  val etlDeps = Seq(
    sparkCore,
    sparkSql,
    sparkHive,
    sparkMl,
    scalaTest,

    // AWS
    hadoopAWS,
    awsJavaSDK,

    // Azure
    hadoopAzure,
    azureStorage,   
    sqlServerJDBC,

    // postgres
    postgresJDBC,

    scallop,
    typesafeConfig,

    scala_graph_core,
    scala_graph_dot,

    // filetypes
    sparkXML,
    sparkAvro,

    // geospark
    geospark,
    geosparkSQL,

    // Azure EventHubs
    azureEventHub,
    qpid

    // hikari connection pool
    // hikaricp

    // gcsConnector
  )
}