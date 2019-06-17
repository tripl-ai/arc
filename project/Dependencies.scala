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

  // Amazon
  val hadoopAWS = "org.apache.hadoop" % "hadoop-aws" % "2.7.7" intransitive()
  val awsJavaSDK = "com.amazonaws" % "aws-java-sdk" % "1.7.4" intransitive()

  // Azure Blob
  val hadoopAzure = "org.apache.hadoop" % "hadoop-azure" % "2.7.3" intransitive()
  val azureStorage = "com.microsoft.azure" % "azure-storage" % "3.1.0" intransitive()

  // Azure EventHubs
  val azureEventHub = "com.microsoft.azure" % "azure-eventhubs" % "1.2.0" intransitive()
  val qpid = "org.apache.qpid" % "proton-j" % "0.29.0" intransitive()

  // Azure AD
  val azureAD = "com.microsoft.azure" % "adal4j" % "1.2.0" intransitive()
  val azureKeyVault = "com.microsoft.azure" % "azure-keyvault" % "1.0.0" intransitive()

  // SQL Server
  val sqlServerJDBC = "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.1.jre8" intransitive()
  val azureSQLDB = "com.microsoft.azure" % "azure-sqldb-spark" % "1.0.2" intransitive()
  val azureCosmosDB = "com.microsoft.azure" %% "azure-cosmosdb-spark_2.4.0" % "1.4.0" intransitive()
  val azureDocumentDB = "com.microsoft.azure" % "azure-documentdb" % "2.4.0" intransitive()

  // Postgres
  val postgresJDBC = "org.postgresql" % "postgresql" % "42.2.5" intransitive()

  // Presto
  val presto = "com.facebook.presto" % "presto-jdbc" % "0.209" intransitive()

  // Mysql
  val mysql = "mysql" % "mysql-connector-java" % "5.1.47" intransitive()

  // cli arg parsing
  val scallop = "org.rogach" %% "scallop" % "2.1.1" intransitive()
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1" intransitive()

  // scala graph
  val scala_graph_core = "org.scala-graph" %% "graph-core" % "1.11.5" intransitive()
  val scala_graph_dot = "org.scala-graph" %% "graph-dot" % "1.11.5" intransitive()
  val scala_graph_json = "org.scala-graph" %% "graph-json" % "1.11.0" intransitive()

  // elasticsearch
  val elasticsearch = "org.elasticsearch" % "elasticsearch-hadoop" % "7.0.1" intransitive()

  // kafka
  val kafka = "org.apache.kafka" %% "kafka" % "2.2.1" intransitive()
  val kafkaClients = "org.apache.kafka" % "kafka-clients" % "2.2.1" intransitive()
  val sparkSQLKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.3" intransitive()

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
    azureSQLDB,
    azureCosmosDB,
    azureDocumentDB,
    postgresJDBC,
    mysql,
    presto,
    scallop,
    typesafeConfig,
    scala_graph_core,
    scala_graph_dot,
    sparkXML,
    sparkAvro,
    azureEventHub,
    qpid,
    elasticsearch,
    kafka,
    kafkaClients,
    sparkSQLKafka
  )
}