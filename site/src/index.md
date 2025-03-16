# Apache Spark and Scala 3

Apache Spark (from now on just Spark) is published for Scala 2.12 and for Scala 2.13.
Therefore, even if not built for Scala 3, your Scala 3 project can depend and use Spark via
[`cross(CrossVersion.for3Use2_13)`](https://www.scala-sbt.org/1.x/docs/Cross-Build.html#Scala+3+specific+cross-versions)

This gives you access to the `RDD` API.

## Spark SQL: `Dataset`

Spark Datasets give us the performances of Dataframes with the addition of type safety.
What happens if we try to use Datasets with Scala 3?

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.*
import org.apache.spark.sql.types.*
import buildinfo.BuildInfo.inputDirectory

@main def wordcountSql =
  val spark = SparkSession.builder().master("local").getOrCreate
  import spark.implicits.*

  val sc = spark.sparkContext

  val textFile = sc.textFile(inputDirectory.getPath + "/lorem-ipsum.txt")
  val words: Dataset[String] = textFile.flatMap(line => line.split(" ")).toDS

  val counts: Dataset[(String, Double)] =
    words
      .map(word => (word, 1d))
      .groupByKey((word, _) => word)
      .reduceGroups((a, b) => (a._1, a._2 + b._2))
      .map(_._2)
```


This will return a cryptic error:

```scala
[error] 22 |        .map(word => (word, 1d))
[error]    |                                ^
[error]    |Unable to find encoder for type (String, Double). An implicit Encoder[(String, Double)] is needed to store (String, Double) instances in a Dataset. Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases..
[error]    |I found:
[error]    |
[error]    |    spark.implicits.newProductEncoder[(String, Double)](
[error]    |      /* missing */summon[reflect.runtime.universe.TypeTag[(String, Double)]])
[error]    |
[error]    |But no implicit values were found that match type reflect.runtime.universe.TypeTag[(String, Double)].
[error] one error found
```

The errors indicates that it cannot find and `implicit Encoder[(String, Double)]` and that we need to
`import spark.implicits._`. But we did that!

## TLDR: How to fix the error with this libray:

 1. Add the library as dependency: `io.github.vincenzobaz" %% "spark-scala3-encoders" % "@VERSION@"`
 2. Import the library after Spark implicits:

```scala
import spark.implicits.*
import scala3encoders.given
```

Read on if you want to know more about how and why this works.

## Understanding the error

The error tells us that the issue occurs on `.map(word => (word, 1d))` because the compiler cannot find a
`implicit Encoder[(String, Double)]`. Let's unpack it:

 - `word => (word, 1d)` is a an anoymous function that produces a `(String, Double)`
 - The signature of `Dataset.map` is:

```scala
class Dataset[T] {
  // More things
  def map[U : Encoder](func: T => U): Dataset[U] = ???
  // More things
}
```
 which we is equivalent to
```scala
class Dataset[T] {
  // More things
  def map[U](func: T => U)(implicit encoder: Encoder[U]): Dataset[U]
  // More things
}
```
 This explains why the compiler is hunting for an `Encoder[(String, Double)]`!

`map` is only one of the functions that require an `Encoder`, have a look at 
the [`Dataset` documentation](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html)
to see more.

## `Encoder`s enable typed code to be efficient

We refer again to the documentation: [`Encoder`](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Encoder.html)

> Used to convert a JVM object of type T to and from the internal Spark SQL representation

`T` in our case is `(String, Dobule)`, which is the type of a JVM object, and that needs to be converted
to the *internal representation*. The motivation is explained in the [SQL section of the Spark guide](https://spark.apache.org/docs/latest/sql-programming-guide.html):

> the interfaces provided by Spark SQL provide Spark with more information about the structure of both the data
> and the computation being performed. Internally, Spark SQL uses this extra information to perform extra optimizations

This is the motivation of Spark SQL: objects expressed in the different languages (Python, Scala, SQL, ...) are
transformed to an internal format, which allows the Spark SQL Engine to understand and optimize queries.
This enables important [performance gains](https://community.cloudera.com/t5/Community-Articles/Spark-RDDs-vs-DataFrames-vs-SparkSQL/ta-p/246547)

## We can finally understand the error

We now know what an `Encoder` and why it is needed.
We still have not cleared why this happens with Scala 3 only.

In Scala 2, we get all of the required implicits, including `Encoder`,
from `import spark.implicits._`: we import all the contents of the 
[`implicits`](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/SparkSession$implicits$.html) 
object.

This, in turn, extends [`SQLImplicits`](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/SQLImplicits.html) which
contains all of the encoder definitions that we need.
Besides the simple ones that return a connstant such as:

```scala
  implicit def newDoubleEncoder: Encoder[Double] = Encoders.scalaDouble
```

we can see more cryptic ones. In our example, we encode a tuple and the error message tells us which encoder it tried `newProductEncoder[(String, Double)]`.

This is defined as 

```scala
  def newProductSeqEncoder[A <: Product : TypeTag]: Encoder[Seq[A]] = ExpressionEncoder()
```

Our tuple is a `Product` and compiler will provide a `TypeTag`, no problem here.
What is this magic `ExpressionEncoder`?

```scala
object ExpressionEncoder {

  def apply[T : TypeTag](): ExpressionEncoder[T] = {
    apply(ScalaReflection.encoderFor[T])
  }
  // More things
```

A couple of go-to-definitions lead us to [this `encoderFor` method](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/catalyst/ScalaReflection.scala#L258).
The core thing to retain is that **this relies on the Scala 2 reflection** API. Therefore this code cannot be
run be compiled by Scala 3. More info on [Cross building a macro library.](https://docs.scala-lang.org/scala3/guides/migration/tutorial-macro-cross-building.html).

## Solution: provide Scala 3 reflection logic to generate encoders

This library implements a layer of Scala 3 reflection logic to replace the one provided by Spark.

Scala 3 metaprogramming allows us to do this elegantly, using the new `inline` mechanisms, meaning that the generation will also
**entirely happen at compile time**, as opposed to the Scala 2 Spark logic which relies on *run-time* reflection.

# Deriving `Encoder`s in Scala 3

## Step 1: build an encoder

The [`Encoders` object](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Encoders$.html) provides us with some
tools to create encoders.

We can use it to build one for our example

```scala
  val spark = SparkSession.builder().master("local").getOrCreate
  val sc = spark.sparkContext
  import spark.implicits.*

  // Our encoder
  val myFirstEncoder: Encoder[(String, Double)] =
    Encoders.tuple[String, Double](strEncoder, Encoders.scalaDouble)

  val textFile = sc.textFile(inputDirectory.getPath + "/lorem-ipsum.txt")
  val words: Dataset[String] = textFile.flatMap(line => line.split(" ")).toDS

  val counts: Dataset[(String, Double)] =
    words
      .map(word => (word, 1d))(myFirstEncoder) // We pass it manually
      .groupByKey((word, _) => word)
      .reduceGroups((a, b) => (a._1, a._2 + b._2))
      .map(_._2)(myFirstEncoder) // We pass it manually
```

We can use Scala's [Contextual abstractions](https://docs.scala-lang.org/scala3/reference/contextual/index.html#the-new-design-1)
to reduce boilerplate code:

```scala
  val spark = SparkSession.builder().master("local").getOrCreate
  val sc = spark.sparkContext
  import spark.implicits.*
  // Our encoder
  given Encoder[(String, Double)] =
    Encoders.tuple[String, Double](Encoders.STRING, Encoders.scalaDouble)

  val textFile = sc.textFile(inputDirectory.getPath + "/lorem-ipsum.txt")
  val words: Dataset[String] = textFile.flatMap(line => line.split(" ")).toDS

  val counts: Dataset[(String, Double)] =
    words
      .map(word => (word, 1d)) // inferred by compiler
      .groupByKey((word, _) => word)
      .reduceGroups((a, b) => (a._1, a._2 + b._2))
      .map(_._2) // inferred by compiler
```

**We see that we can define custom `Encoder`s without relying on `spark.implicits` and that we
can use contextual abstractions to propagate them without code changes.**

## Step 2: Generalizing

### Ingredients

Our goal is to replace the logic of `ExpressionEncoder()`, based on Scala 2 reflection, with Scala 3 compatible logic.

What do we need to create `ExpressionEncoder`s?

```scala
case class ExpressionEncoder[T](
    objSerializer: Expression,
    objDeserializer: Expression,
    clsTag: ClassTag[T])
  extends Encoder[T] { /*...*/ }
```

`ClassTag` are generated by the compiler, so we only need to propagate them.
What are `obSerializer` and `objDeserializer`? And what is `Expresion`?

As we mentioned above, encoders transform objects from Scala/Python/Java into internal representations.
If you have ever worked with JSON or other serialization formats, you might have encountered this *SerDe* pattern:
we separate the logic required to turn your object into the target format (*Serializer*) from the logic
required to turn an object into the target format into an object you can manipulate in your language
(*Deserializer*).

A good example is [circe's `Codec`](https://circe.github.io/circe/api/io/circe/Codec.html) which is the
the product of `Encoder` and `Decoder` where:

```scala
trait Encoder[A] extends Serializable { self =>
  def apply(a: A): Json
  // more
}

trait Decoder[A] extends Serializable { self =>
  def apply(c: HCursor): Decoder.Result[A]
  // more
}
```

Unlike in circe, our serde logic is written in `Expression`.
Since the logic is executed by Spark SQL internal engine, it needs
to be written in a language that the engine understands.

This language is defined in the `org.apache.spark.sql.catalyst` package
since [Catalyst](https://www.databricks.com/glossary/catalyst-optimizer)
is the optimizer that takes our Spark SQL code, optimizes it and emits code to run.

### Learning a new language?

Do we need to learn this new language AND write custom expressions in it?

We do not. Remember that Spark already contains all of the logic and definitions
for the encoders. The only part that we need to do is to create a layer that bridges
Scala 3 user code to these definitions.

Let's consider our `Double`: how do we write an expression to encode it? We look into Spark!
We can find it [here](https://github.com/apache/spark/blob/39542bb81f8570219770bb6533c077f44f6cbd2a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/SerializerBuildHelper.scala#L55-L57)

The same idea applies for deseriliazers.

Now that we know where to find the logic, we can focus on organizing our codebase
to generate `ExpressionEncoder`s without requiring code changes.


### Library structure

The entrypoint is:

```scala
given encoder[T](using
    serializer: Serializer[T],
    deserializer: Deserializer[T],
    classTag: ClassTag[T]
): ExpressionEncoder[T] = ???
```

where `Serializer` and `Deserializer` are two abstractions defined in this library.
They wrap the `Expression` objects that we have just mentioned.

Let's focus on `Serializer` to better understand how the derivation works.
The companion object of this class defines instances for the simple types that we saw
in the example above (`String`, `Double`):

```scala
  given Serializer[Double] with
    def inputType: DataType = DoubleType
    def serialize(inputObject: Expression): Expression = inputObject

  given Serializer[String] with
    def inputType: DataType = ObjectType(classOf[String])
    def serialize(inputObject: Expression): Expression =
      createSerializerForString(inputObject)
```

but also more complex types, such as collections or products.
I like these two examples for different reasons:

 - The `Seq` derivation shows the expressive power of Scala's type system
 - The `Product` derivation shows how to iterate on `Tuple`s in Scala 3

#### Serializing a `Seq`

to serialize a `Seq`uence of objects I need to know:

 - that I am working with a sequence
 - how to encode the elements inside the sequence

These two constraints are expressed in the type of the derivation:

```scala
  given deriveSeq[F[_], T](using objectSerializer: Serializer[T])(using
      F[T] <:< Seq[T]
  ): Serializer[F[T]]
```

#### Serializing a `Product`

The product serializer uses the new [Mirror](https://docs.scala-lang.org/scala3/reference/contextual/derivation.html#mirror-1)
types from Scala 3:

```scala
  inline given derivedProduct[T](using
      mirror: Mirror.ProductOf[T],
      classTag: ClassTag[T]
  ): Serializer[T] =
```

Since the compiler generates mirrors only for products, the `using` constraint here
means that this code will only be invoked for products.
`ProductOf` lets us inspect the types of the elements that form `T` and treat the product
as a tuple. In Scala 3 tuples are very powerful! 
I wrote [an introduction to tuples](https://www.scala-lang.org/2021/02/26/tuples-bring-generic-programming-to-scala-3.html).
`inline` has also become more powerful in Scala 3, read more [here](https://docs.scala-lang.org/scala3/guides/macros/inline.html)
