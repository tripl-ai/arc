// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "ai.tripl"

// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

licenses := Seq("MIT" -> new URL("https://opensource.org/licenses/MIT"))

import xerial.sbt.Sonatype._

homepage := Some(url("https://arc.tripl.ai"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/tripl-ai/arc"),
    "scm:git@github.com:tripl-ai/arc.git"
  )
)
developers := List(
  Developer(id="jbruce", name="John Bruce", email="john@tripl.ai", url=url("https://github.com/jbruce")),
  Developer(id="seddonm1", name="Mike Seddon", email="mike@tripl.ai", url=url("https://github.com/seddonm1"))
)

credentials += Credentials("Sonatype Nexus Repository Manager",
        "oss.sonatype.org",
        sys.env.get("SONATYPE_USERNAME").getOrElse(""),
        sys.env.get("SONATYPE_PASSWORD").getOrElse(""))