import Dependencies._

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  configs(IntegrationTest).
  settings(
    name := "arc",
    organization := "ai.tripl",
    scalaVersion := "2.11.12",
    scalastyleFailOnError := false,
    libraryDependencies ++= etlDeps,
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    buildInfoKeys := Seq[BuildInfoKey](version, scalaVersion),
    buildInfoPackage := "ai.tripl.arc.ArcBuildInfo",
    Defaults.itSettings
  )

fork in run := true  

test in assembly := {}

assemblyJarName in assembly := s"${name.value}.jar"

scalacOptions := Seq("-target:jvm-1.8", "-unchecked", "-deprecation")

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
    // this match removes META-INF files except for the ones for plugins
    case PathList("META-INF", xs @ _*) =>
      xs match {
        case "services" :: xs => MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.discard
      }
    case x => MergeStrategy.first
   }
}