# Spark-Scala3

This repository aims at making the adoption of Scala 3 easier for projects using
Apache Spark.

## How to get this library

Add the following dependency to your `build.sbt`:

```
"io.github.vincenzobaz" %% "spark-scala3" % "0.1.2"
```

## Apache Spark version

As Scala 3 can use Scala 2.13 libraries, your Spark-Scala3 project needs a Spark
version published for 2.13. Spark 3.2+ is compatible with Scala 2.13.

You can add it in your `build.sbt` with:

```scala
ThisBuild / resolvers += "Spark Snapshots" at "https://repository.apache.org/content/repositories/snapshots"

val sparkVersion = "3.2.0"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13)
)
```

## Encoders

The Spark `sql` API provides the `Dataset[T]` abstraction. Most methods of this
type require an implicit (`given` in Scala 3 parlance) instance of `Encoder[T]`.

These instances are automatically derived when you use:

```
scala import sqlContext.implicits._
```

Unfortunately this derivation relies on Scala 2.13 runtime reflection which is
not supported in Scala 3.

This library provides you with an alternative compile time encoder derivation which
you can enable with the following import:

```scala
import scala3encoders.given
```




