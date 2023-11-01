# Spark-Scala3

This repository aims at making the adoption of Scala 3 easier for projects using
Apache Spark.

## How to get this library

Add the following dependency to your `build.sbt`:

```
"io.github.vincenzobaz" %% "spark-scala3-encoders" % "0.2.5"
"io.github.vincenzobaz" %% "spark-scala3-udf" % "0.2.5"
```

## Apache Spark version

As of version 3.2, Spark is published for Scala 2.13, which makes it possible to be used from Scala 3 as a library.

You can add it in your `build.sbt` with:

```scala
val sparkVersion = "3.3.2"

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

## Udf

`Udf` or "user defined functions" enable Spark to use own functions that process rows.
The main routine for creating and registering those functions are 
`spark.sql.functions.udf` and `SparkSession.register`. These cannot be replaced as simply as the encoders.
The `udf` call itself is a generic function using `TypeTags` which are not available in Scala 3. The `register` is needed to make the calls available inside a `spark.sql` statement with a string identifier.

To use spark-scala3 `udf`:

```scala
import scala3udf.{Udf => udf} // "old" udf doesn't interfer with new scala3udf.udf when renamed
``` 

With the renaming in place, you can use the call `udf(lambda)` as before without interfering with the `udf` function in `org.apache.spark.sql.functions`. Instead of calling `spark.register(myFun1)`, you can call either:

- `myFun1.registerWith(spark, "myFun1")`
- `udf.registerWith(spark, myFun1, myFun2, ...)` - this will automatically name the used parameter value names
Instead of an explicit Spark session an implicit value could also be used, e.g. using `given spark: SparkSession = SparkSession.builder()...getOrCreate`. Then the equivalent calls are:

- `myFun1.register("myFun1")`
- `udf.register(myFun1, myFun2, ...)`

Type checks will not work with Spark 3.3.x - this could lead to unexpected failures, e.g. of the `collect()` function. The recommendation is to use spark version at least 3.4.1.
