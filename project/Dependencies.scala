import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "2.4.3"
  lazy val scalaTestVersion = "3.0.7"

  // arc
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1" intransitive()  
  val scala_graph_core = "org.scala-graph" %% "graph-core" % "1.11.5" intransitive()
  val scala_graph_dot = "org.scala-graph" %% "graph-dot" % "1.11.5" intransitive()
  val scala_graph_json = "org.scala-graph" %% "graph-json" % "1.11.0" intransitive()  

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test,it"
  val jetty = "org.mortbay.jetty" % "jetty" % "6.1.26" % "test,it"
  val hadoopAWS = "org.apache.hadoop" % "hadoop-aws" % "2.7.7" % "test,it"
  val awsJavaSDK = "com.amazonaws" % "aws-java-sdk" % "1.7.4" % "test,it"
  val postgresJDBC = "org.postgresql" % "postgresql" % "42.2.5" % "test,it" 

  // spark
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided" 
  val sparkMl = "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"

  // spark XML
  val sparkXML = "com.databricks" %% "spark-xml" % "0.5.0" intransitive()

  // spark AVRO
  val sparkAvro = "org.apache.spark" %% "spark-avro" % sparkVersion 

  // delta
  val deltaLake = "io.delta" %% "delta-core" % "0.2.0" intransitive()

  // Project
  val etlDeps = Seq(
    typesafeConfig,
    scala_graph_core,
    scala_graph_dot,
    scalaTest,
    jetty,
    hadoopAWS,
    awsJavaSDK,    
    postgresJDBC,
    sparkCore,
    sparkSql,
    sparkHive,
    sparkMl,
    sparkXML,
    sparkAvro,    
    deltaLake
  )
}