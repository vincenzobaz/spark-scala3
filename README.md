# Spark-Scala3

This repository aims at making the adoption of Scala 3 easier for projects using
Apache Spark.

## How to get this library

Add the following dependency to your `build.sbt`:

```
"io.github.vincenzobaz" %% "spark-scala3" % "0.1.5"
```

## Apache Spark version

As of version 3.2, Spark is published for Scala 2.13, which makes it possible to be used from Scala 3 as a library.

You can add it in your `build.sbt` with:

```scala
val sparkVersion = "3.3.1"

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
