ThisBuild / scalaVersion := "3.0.0-RC2-bin-20210319-b1f0b30-NIGHTLY"
ThisBuild / resolvers += "Spark Snapshots" at "https://repository.apache.org/content/repositories/snapshots"

val sparkVersion = "3.2.0-SNAPSHOT"
val sparkCore = ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13)
val sparkSql = ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13)
val munit = "org.scalameta" %% "munit" % "0.7.22"

lazy val encoders = project
  .in(file("encoders"))
  .settings(
    libraryDependencies ++= Seq(
      sparkSql,
      munit % Test
    )
  )

lazy val examples = project
  .in(file("examples"))
  .dependsOn(encoders)
  .settings(
    run / fork := true
  )
