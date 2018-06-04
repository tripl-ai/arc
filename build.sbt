import Dependencies._

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    name := "arc",
    organization := "au.com.agl",
    scalaVersion := "2.11.8",
    scalastyleFailOnError := false,
    libraryDependencies ++= etlDeps,
    parallelExecution in Test := false,
    buildInfoKeys := Seq[BuildInfoKey](version, scalaVersion),
    buildInfoPackage := "au.com.agl.arc" 
  )

test in assembly := {}

assemblyJarName in assembly := s"${name.value}.jar"

scalacOptions := Seq("-unchecked", "-deprecation")

// META-INF discarding
assemblyMergeStrategy in assembly := {
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}

// allow netty for spark (old) and netty (less old) for tensorflow grpc calls to co-exist
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shadeio.@1").inAll,
  ShadeRule.rename("com.google.guava.**" -> "shadeio.@1").inAll,
  ShadeRule.rename("com.google.common.**" -> "shadeio.@1").inAll,
  ShadeRule.rename("io.netty.**" -> "shadeio.@1").inAll
)