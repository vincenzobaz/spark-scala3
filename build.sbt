import sbt.internal.ProjectMatrix

val scalaVer = "3.3.1"
ThisBuild / scalaVersion := scalaVer
ThisBuild / semanticdbEnabled := true
ThisBuild / scalacOptions ++= List(
  "-Wunused:imports"
)

val munit = "org.scalameta" %% "munit" % "0.7.29"

val inputDirectory = Def.settingKey[File]("")

def sparkSqlDep(ver: String) =
  ("org.apache.spark" %% "spark-sql" % ver).cross(CrossVersion.for3Use2_13)

// Spark versions to check. Always most recent first.
lazy val sparkVersions = List(
  (SparkVersionAxis("_spark350", "spark350"), "3.5.0", false),
  (SparkVersionAxis("_spark332", "spark332"), "3.3.2", true)
)

def sparkVersionMatrix(
    projectRoot: ProjectMatrix,
    overrideSkipPublish: Boolean = false,
    sparkProvided: Boolean = true
): ProjectMatrix = {
  sparkVersions.foldLeft(projectRoot) {
    case (acc, (axis, version, skipPublish)) =>
      acc.customRow(
        scalaVersions = Seq(scalaVer),
        axisValues = Seq(axis, VirtualAxis.jvm),
        _.settings(
          moduleName := name.value + axis.idSuffix,
          publish / skip := (overrideSkipPublish || skipPublish),
          libraryDependencies += (if (sparkProvided)
                                    sparkSqlDep(version) % Provided
                                  else sparkSqlDep(version))
        )
      )
  }
}

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

lazy val root = (project in file("."))
  .aggregate(
    ((encoders.projectRefs) ++ udf.projectRefs ++ examples.projectRefs): _*
  )
  .settings(
    publishSettings,
    publish / skip := true
  )

lazy val encoders = sparkVersionMatrix(projectMatrix in file("encoders"))
  .settings(
    name := "spark-scala3-encoders",
    libraryDependencies += munit % Test,
    Test / fork := true,
    Test / javaOptions ++= unnamedJavaOptions
    // Test / javaOptions += "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=1044"
  )
  .settings(publishSettings)

lazy val udf = sparkVersionMatrix(projectMatrix in file("udf"))
  .in(file("udf"))
  .settings(
    name := "spark-scala3-udf",
    libraryDependencies += munit % Test,
    Test / fork := true,
    Test / javaOptions ++= unnamedJavaOptions
  )
  .settings(publishSettings)
  .dependsOn(encoders)

lazy val examples =
  sparkVersionMatrix(
    projectMatrix in file("examples"),
    overrideSkipPublish = true,
    sparkProvided = false
  )
    .enablePlugins(BuildInfoPlugin)
    .dependsOn(encoders, udf)
    .settings(
      publish / skip := true,
      inputDirectory.withRank(
        KeyRanks.Invisible
      ) := baseDirectory.value / "input",
      buildInfoKeys := Seq[BuildInfoKey](inputDirectory),
      run / fork := true,
      run / javaOptions ++= unnamedJavaOptions
    )

addCommandAlias(
  "runMainExample",
  s"${examples.finder(sparkVersions.head._1, VirtualAxis.jvm)(scalaVer).id}/runMain"
)

import xerial.sbt.Sonatype._
lazy val publishSettings = Def.settings(
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
