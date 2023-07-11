ThisBuild / scalaVersion := "3.2.2"
ThisBuild / semanticdbEnabled := true

val sparkVersion = "3.3.2"
val sparkCore = ("org.apache.spark" %% "spark-core" % sparkVersion).cross(
  CrossVersion.for3Use2_13
)
val sparkSql = ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(
  CrossVersion.for3Use2_13
)
val munit = "org.scalameta" %% "munit" % "0.7.29"

val inputDirectory = Def.settingKey[File]("")

// See https://github.com/apache/spark/blob/v3.3.2/launcher/src/main/java/org/apache/spark/launcher/JavaModuleOptions.java
val unnamedJavaOptions = List(
  "-XX:+IgnoreUnrecognizedVMOptions",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

lazy val root = project
  .in(file("."))
  .aggregate(encoders, udf, examples)
  .settings(publish / skip := true)
  .settings(publishSettings)

lazy val encoders = project
  .in(file("encoders"))
  .settings(
    name := "spark-scala3-encoders",
    libraryDependencies ++= Seq(sparkSql, munit % Test),
    Test / fork := true,
    Test / javaOptions ++= unnamedJavaOptions
    // Test / javaOptions += "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=1044"
  )
  .settings(publishSettings)

lazy val udf = project
  .in(file("udf"))
  .settings(
    name := "spark-scala3-udf",
    libraryDependencies ++= Seq(sparkSql, munit % Test),
    Test / fork := true,
    Test / javaOptions ++= unnamedJavaOptions
  )
  .settings(publishSettings)
  .dependsOn(encoders)

lazy val examples = project
  .in(file("examples"))
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(encoders, udf)
  .settings(
    publish / skip := true,
    inputDirectory.withRank(
      KeyRanks.Invisible
    ) := baseDirectory.value / "input",
    buildInfoKeys := Seq[BuildInfoKey](inputDirectory),
    libraryDependencies ++= Seq(sparkSql),
    run / fork := true,
    run / javaOptions ++= unnamedJavaOptions
  )
  .settings(publishSettings)

import xerial.sbt.Sonatype._
lazy val publishSettings = Def.settings(
  name := "spark-scala3",
  licenses := Seq(
    "APL2" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")
  ),
  organization := "io.github.vincenzobaz",
  homepage := Some(url("https://github.com/vincenzobaz/spark-scala3")),
  sonatypeRepository := "https://s01.oss.sonatype.org/service/local",
  sonatypeCredentialHost := "s01.oss.sonatype.org",
  publishTo := sonatypePublishToBundle.value,
  developers := List(
    Developer(
      "vincenzobaz",
      "Vincenzo Bazzucchi",
      "bazzucchi.vincenzo@gmail.com",
      url("https://github.com/vincenzobaz/")
    ),
    Developer(
      "adpi2",
      "Adrien Piquerez",
      "adrien.piquerez@gmail.com",
      url("https://github.com/adpi2")
    ),
    Developer(
      "michael72",
      "Michael Schulte",
      "michael.schulte@gmx.org",
      url("https://github.com/michael72")
    )
  ),
  sonatypeProjectHosting := Some(
    GitHubHosting("vincenzobaz", name.value, "bazzucchi.vincenzo@gmail.com")
  ),
  versionScheme := Some("early-semver"),
  versionPolicyIntention := Compatibility.None
)
