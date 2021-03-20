scalaVersion := "3.0.0-RC2-bin-20210319-b1f0b30-NIGHTLY"
resolvers += "Spark Snapshots" at "https://repository.apache.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "3.2.0-SNAPSHOT").cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-sql" % "3.2.0-SNAPSHOT").cross(CrossVersion.for3Use2_13)
)

run / fork := true
