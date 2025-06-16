import sbt.internal.ProjectMatrix

val scalaVer = "3.3.6"
ThisBuild / scalaVersion := scalaVer
ThisBuild / semanticdbEnabled := true
ThisBuild / scalacOptions ++= List(
  "-Wunused:imports"
)

val munit = "org.scalameta" %% "munit" % "0.7.29"

val inputDirectory = Def.settingKey[File]("")

def sparkSqlDep(ver: String) =
  ("org.apache.spark" %% "spark-sql" % ver).cross(CrossVersion.for3Use2_13)

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
  .aggregate(udf3)
  .aggregate(udf4)
  .aggregate(encoders3)
  .aggregate(encoders4)
  .aggregate(examples.projectRefs: _*)
  .settings(
    publishSettings,
    publish / skip := true
  )

lazy val encoders3 = project
  .in(file("encoders"))
  .settings(
    name := "spark-scala3-encoders",
    libraryDependencies ++= Seq(
      sparkSqlDep(spark3Versions.head.sparkVersion),
      munit % Test
    ),
    Test / fork := true,
    Test / javaOptions ++= unnamedJavaOptions
    // Test / javaOptions += "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=1044"
  )
  .settings(publishSettings)

lazy val udf3 = project
  .in(file("udf"))
  .settings(
    name := "spark-scala3-udf",
    libraryDependencies ++= Seq(
      sparkSqlDep(spark3Versions.head.sparkVersion),
      munit % Test
    ),
    Test / fork := true,
    Test / javaOptions ++= unnamedJavaOptions
  )
  .settings(publishSettings)
  .dependsOn(encoders3)

lazy val encoders4 = project
  .in(file("encoders4"))
  .settings(
    name := "spark4-scala3-encoders",
    libraryDependencies ++= Seq(
      sparkSqlDep(spark4Versions.head.sparkVersion),
      munit % Test
    ),
    Test / fork := true,
    Test / javaOptions ++= unnamedJavaOptions
    // Test / javaOptions += "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=1044"
  )
  .settings(publishSettings)

lazy val udf4 = project
  .in(file("udf4"))
  .settings(
    name := "spark4-scala3-udf",
    libraryDependencies ++= Seq(
      sparkSqlDep(spark4Versions.head.sparkVersion),
      munit % Test
    ),
    Test / fork := true,
    Test / javaOptions ++= unnamedJavaOptions
  )
  .settings(publishSettings)
  .dependsOn(encoders4)

lazy val examples =
  sparkVersionMatrix(
    projectMatrix.in(file("examples"))
  )
    .enablePlugins(BuildInfoPlugin)
    .settings(
      publish / skip := true,
      inputDirectory.withRank(
        KeyRanks.Invisible
      ) := (ThisBuild / baseDirectory).value / "examples" / "input",
      buildInfoKeys := Seq[BuildInfoKey](inputDirectory),
      run / fork := true,
      run / javaOptions ++= unnamedJavaOptions
    )

addCommandAlias(
  "latestExample",
  s"${examples.finder(spark4Versions.head, VirtualAxis.jvm)(scalaVer).id}"
)

lazy val spark4Versions = List(
  SparkVersionAxis("_spark40_", "spark400", "4.0.0")
)

// Spark versions to check. Always most recent first.
lazy val spark3Versions = List(
  SparkVersionAxis("_spark35_", "spark350", "3.5.5"),
  SparkVersionAxis("_spark34_", "spark341", "3.4.4"),
  SparkVersionAxis("_spark33_", "spark333", "3.3.3")
)

lazy val runAllMains = taskKey[Unit]("Run all mains")
runAllMains := Def.sequential {
  examples.allProjects().map(_._1).map { project =>
    Def.taskDyn {
      val mainClasses = (project / Compile / discoveredMainClasses).value
      Def.sequential {
        mainClasses.map { mainClass =>
          (project / Compile / runMain).toTask(" " + mainClass)
        }
      }
    }
  }
}.value

def sparkVersionMatrix(
    projectRoot: ProjectMatrix
): ProjectMatrix = {
  (spark3Versions ++ spark4Versions).foldLeft(projectRoot) { case (acc, axis) =>
    acc.customRow(
      scalaVersions = Seq(scalaVer),
      axisValues = Seq(axis, VirtualAxis.jvm),
      _.settings(
        moduleName := name.value + axis.idSuffix,
        publish / skip := true,
        libraryDependencies += sparkSqlDep(axis.sparkVersion)
      ).dependsOn(
        if (axis.sparkVersion.startsWith("4.")) encoders4 else encoders3,
        if (axis.sparkVersion.startsWith("4.")) udf4 else udf3
      )
    )
  }
}

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

import laika.helium.config._
import laika.ast.Path.Root
lazy val docs = project
  .in(file("site"))
  .enablePlugins(TypelevelSitePlugin)
  .settings(
    mdocIn := baseDirectory.value / "src",
    tlSiteHelium := tlSiteHelium.value.site.topNavigationBar(
      homeLink = IconLink.internal(Root / "index.md", HeliumIcon.home)
    )
  )
