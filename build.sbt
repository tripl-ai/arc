import Dependencies._

lazy val scala212 = "2.12.17"
lazy val supportedScalaVersions = List(scala212)

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  configs(IntegrationTest).
  settings(
    name := "arc",
    organization := "ai.tripl",
    organizationHomepage := Some(url("https://arc.tripl.ai")),
    crossScalaVersions := supportedScalaVersions,
    licenses := List("MIT" -> new URL("https://opensource.org/licenses/MIT")),
    scalastyleFailOnError := false,
    libraryDependencies ++= etlDeps,
    Test / parallelExecution := false,
    IntegrationTest / parallelExecution := false,
    buildInfoKeys := Seq[BuildInfoKey](version, scalaVersion),
    buildInfoPackage := "ai.tripl.arc.ArcBuildInfo",
    Defaults.itSettings,
    publishTo := sonatypePublishTo.value,
    pgpPassphrase := Some(sys.env.get("PGP_PASSPHRASE").getOrElse("").toCharArray),
    pgpSecretRing := file("/pgp/secring.asc"),
    pgpPublicRing := file("/pgp/pubring.asc"),
    updateOptions := updateOptions.value.withGigahorse(false)
  )

resolvers += Resolver.mavenLocal
publishM2Configuration := publishM2Configuration.value.withOverwrite(true)

// resolvers += "Spark Staging" at "https://repository.apache.org/content/repositories/orgapachespark-1367/"

run / fork := true

scalacOptions := Seq(
  "-deprecation",
  "-encoding", "utf-8",
  "-explaintypes",
  "-target:jvm-1.8",
  "-unchecked",
  "-feature",

  "-Ywarn-dead-code",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any"
  // "-Ywarn-unused"
)

javacOptions += "-Xlint:unchecked"

assembly / test := {}

// META-INF discarding
assembly / assemblyMergeStrategy := {
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
