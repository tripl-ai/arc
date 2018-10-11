import Dependencies._

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  configs(IntegrationTest).
  settings(
    name := "arc",
    organization := "au.com.agl",
    scalaVersion := "2.11.8",
    scalastyleFailOnError := false,
    libraryDependencies ++= etlDeps,
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    buildInfoKeys := Seq[BuildInfoKey](version, scalaVersion),
    buildInfoPackage := "au.com.agl.arc",
    Defaults.itSettings
  )

test in assembly := {}

assemblyJarName in assembly := s"${name.value}.jar"

scalacOptions := Seq("-unchecked", "-deprecation")

// exclude from build as they are in the dockerfile
assemblyExcludedJars in assembly := { 
  val cp = (fullClasspath in assembly).value
  cp filter {c => 
    c.data.getName == "kafka_2.11-1.1.0.jar"
    c.data.getName == "kafka-clients-1.1.0.jar"
  }
}

// META-INF discarding
assemblyMergeStrategy in assembly := {
   {
    case PathList("META-INF", xs @ _*) =>
      xs match {
        case "services" :: xs => MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.discard
      }
    case x => MergeStrategy.first
   }
}