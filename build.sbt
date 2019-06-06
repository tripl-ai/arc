import Dependencies._

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  configs(IntegrationTest).
  settings(
    name := "arc",
    organization := "ai.tripl",
    organizationHomepage := Some(url("https://arc.tripl.ai")),
    scalaVersion := "2.11.12",
    licenses := List("MIT" -> new URL("https://opensource.org/licenses/MIT")),
    scalastyleFailOnError := false,
    libraryDependencies ++= etlDeps,
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    buildInfoKeys := Seq[BuildInfoKey](version, scalaVersion),
    buildInfoPackage := "ai.tripl.arc.ArcBuildInfo",
    Defaults.itSettings,
    publishTo := sonatypePublishTo.value,
    pgpPassphrase := Some(sys.env.get("PGP_PASSPHRASE").getOrElse("").toCharArray),
    pgpSecretRing := file("/pgp/secring.asc"),
    pgpPublicRing := file("/pgp/pubring.asc"),
    updateOptions := updateOptions.value.withGigahorse(false)
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

