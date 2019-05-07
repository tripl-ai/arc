import sbt._

object Dependencies {
  // Versions
  lazy val sparkVersion = "2.4.2"
  lazy val scalaTestVersion = "3.0.7"
  lazy val includeJars = if (Option(System.getProperty("assemblyTarget")).getOrElse("standalone") == "databricks") "compile" else "provided"

  // Testing
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "it,test"
  val scalactic = "org.scalactic" %% "scalactic" % scalaTestVersion % "it,test"

  // Spark
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided" 
  val sparkMl = "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"

  // Spark XML
  val sparkXML = "com.databricks" %% "spark-xml" % "0.5.0" % includeJars

  // Spark AVRO
  val sparkAvro = "org.apache.spark" %% "spark-avro" % "2.4.0" % includeJars

  // Amazon S3
  val hadoopAWS = "org.apache.hadoop" % "hadoop-aws" % "2.7.7" % includeJars
  val awsJavaSDK = "com.amazonaws" % "aws-java-sdk" % "1.7.4" % includeJars

  // Azure Blob
  val hadoopAzure = "org.apache.hadoop" % "hadoop-azure" % "2.7.3" % includeJars
  val azureStorage = "com.microsoft.azure" % "azure-storage" % "3.1.0" % includeJars

  // Azure EventHubs
  val azureEventHub = "com.microsoft.azure" % "azure-eventhubs" % "1.2.0" % includeJars
  val qpid = "org.apache.qpid" % "proton-j" % "0.29.0" % includeJars

  // Azure AD
  val azureAD = "com.microsoft.azure" % "adal4j" % "1.2.0" % includeJars
  val azureKeyVault = "com.microsoft.azure" % "azure-keyvault" % "1.0.0" % includeJars

  // SQL Server
  val sqlServerJDBC = "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.1.jre8" % includeJars
  val azureSQLDB = "com.microsoft.azure" % "azure-sqldb-spark" % "1.0.2" % includeJars
  val postgresJDBC = "org.postgresql" % "postgresql" % "42.2.5" % includeJars

  // cli arg parsing
  val scallop = "org.rogach" %% "scallop" % "2.1.1"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1"

  // scala graph
  val scala_graph_core = "org.scala-graph" %% "graph-core" % "1.11.5"
  val scala_graph_dot = "org.scala-graph" %% "graph-dot" % "1.11.5"
  val scala_graph_json = "org.scala-graph" %% "graph-json" % "1.11.0"

  // elasticsearch
  val elasticsearch = "org.elasticsearch" % "elasticsearch-hadoop" % "6.6.1" % includeJars

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
    azureSQLDB,

    // postgres
    postgresJDBC,

    scallop,
    typesafeConfig,

    scala_graph_core,
    scala_graph_dot,

    // filetypes
    sparkXML,
    sparkAvro,

    // Azure EventHubs
    azureEventHub,
    qpid,

    // elasticsearch
    elasticsearch


  )
}