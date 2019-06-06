// better ivy / maven jar downloading
addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.1.0-M13-4")

// style
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

// versioninfo
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")

// license
addSbtPlugin("com.typesafe.sbt" % "sbt-license-report" % "1.2.0")

// publish to maven
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.5")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.1")
