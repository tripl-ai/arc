import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "3.3.0"
  lazy val hadoopVersion = "3.3.4"

  // arc
  val typesafeConfig = "com.typesafe" % "config" % "1.4.0"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.9" % "test,it"
  val jetty = "org.mortbay.jetty" % "jetty" % "6.1.26" % "test,it"
  val postgresJDBC = "org.postgresql" % "postgresql" % "42.5.0" % "test,it"
  val sqlServerJDBC = "com.microsoft.sqlserver" % "mssql-jdbc" % "7.4.1.jre8" % "it"

  // spark
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  val sparkMl = "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
  val sparkAvro = "org.apache.spark" %% "spark-avro" % sparkVersion % "provided"

  // hadoop
  val hadoopCommon =  "org.apache.hadoop" % "hadoop-common" % hadoopVersion
  val hadoopAWS = "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % "provided"

  // Project
  val etlDeps = Seq(
    typesafeConfig,
    scalaTest,
    jetty,
    hadoopCommon,
    hadoopAWS,
    postgresJDBC,
    sqlServerJDBC,
    sparkCore,
    sparkSql,
    sparkHive,
    sparkMl,
    sparkAvro
  )
}