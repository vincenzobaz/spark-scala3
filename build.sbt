ThisBuild / scalaVersion := "3.1.1"
ThisBuild / semanticdbEnabled := true

val sparkVersion = "3.3.0"
val sparkCore = ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13)
val sparkSql = ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13)
val munit = "org.scalameta" %% "munit" % "0.7.26"

val inputDirectory = Def.settingKey[File]("")

lazy val root = project
  .in(file("."))
  .aggregate(encoders, examples)
  .settings(publish / skip := true)
  .settings(publishSettings)

lazy val encoders = project
  .in(file("encoders"))
  .settings(
    libraryDependencies ++= Seq(sparkSql, munit % Test),
    Test / parallelExecution := false,
    // Test / fork := truek
    // Test / javaOptions += "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=1044"
  ).settings(publishSettings)

lazy val examples = project
  .in(file("examples"))
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(encoders)
  .settings(
    publish / skip := true,
    inputDirectory := baseDirectory.value / "input",
    buildInfoKeys := Seq[BuildInfoKey](inputDirectory),
    run / fork := true
  ).settings(publishSettings)

import xerial.sbt.Sonatype._
lazy val publishSettings = Def.settings(
  name := "spark-scala3",
  licenses := Seq("APL2" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
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
    )
  ),
  sonatypeProjectHosting := Some(GitHubHosting("vincenzobaz", name.value, "bazzucchi.vincenzo@gmail.com")),
  versionScheme := Some("early-semver"),
  versionPolicyIntention := Compatibility.None
)
