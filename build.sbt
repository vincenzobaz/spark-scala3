ThisBuild / scalaVersion := "3.0.0"
ThisBuild / name := "spark-scala3"
ThisBuild / semanticdbEnabled := true
ThisBuild / resolvers += "Spark Snapshots" at "https://repository.apache.org/content/repositories/snapshots"

val sparkVersion = "3.2.0-SNAPSHOT"
val sparkCore = ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13)
val sparkSql = ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13)
val munit = "org.scalameta" %% "munit" % "0.7.26"

val inputDirectory = Def.settingKey[File]("")


lazy val encoders = project
  .in(file("encoders"))
  .settings(
    name := "spark-scala3",
    organization := "io.vincenzobaz",
    libraryDependencies ++= Seq(sparkSql, munit % Test),
    Test / parallelExecution := false,
    // Test / fork := true,
    // Test / javaOptions += "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=1044"
  )

lazy val examples = project
  .in(file("examples"))
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(encoders)
  .settings(
    publish / skip := true,
    inputDirectory := baseDirectory.value / "input",
    buildInfoKeys := Seq[BuildInfoKey](inputDirectory),
    run / fork := true
  )

inThisBuild(List(
  licenses := Seq("APL2" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
  organization := "io.github.vincenzobaz",
  homepage := Some(url("https://github.com/vincenzobaz/spark-scala3")),
  sonatypeCredentialHost := "s01.oss.sonatype.org",
  compile/doc/sources := Seq(),
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
  )
))