scalaVersion := dottyLatestNightlyBuild.get

resolvers += "Spark Snapshots" at "https://repository.apache.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "3.2.0-SNAPSHOT").withDottyCompat(scalaVersion.value),
  ("org.apache.spark" %% "spark-sql" % "3.2.0-SNAPSHOT").withDottyCompat(scalaVersion.value)
)

run / fork := true

