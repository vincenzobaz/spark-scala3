package sql

import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.sql.types._

class EncoderDerivationSpec extends munit.FunSuite:
  import EncoderDerivation.given

  test("derive schema of case class Foo()") {
    case class Foo()
    assertEquals(summon[Encoder[Foo]].schema, StructType(Seq.empty))
  }

  test("derive schema of case class Foo(x: String)") {
    case class Foo(x: String)
    val expected = StructType(Seq(StructField("x", StringType)))
    assertEquals(summon[Encoder[Foo]].schema, expected)
  }

  test("derive schema of case class Foo(x: String, y: Long)") {
    case class Foo(x: String, y: Long)
    val expected = StructType(
      Seq(
        StructField("x", StringType),
        StructField("y", LongType)
      )
    )
    assertEquals(summon[Encoder[Foo]].schema, expected)
  }

  test("derive schema of case class Foo(x: String, y: Bar) where Bar is case class Bar(x: Int)") {
    case class Foo(x: String, y: Bar)
    case class Bar(x: Int)
    val expected = StructType(
      Seq(
        StructField("x", StringType),
        StructField("y", IntegerType)
      )
    )
    assertEquals(summon[Encoder[Foo]].schema, expected)
  }

  test("derive schema of case class Foo(x: String, y: Bar) where Bar is case class Bar(x: Int, y: Long)") {
    case class Foo(x: String, y: Bar)
    case class Bar(x: Int, y: Long)
    val expected = StructType(
      Seq(
        StructField("x", StringType),
        StructField(
          "y",
          StructType(
            Seq(
              StructField("x", IntegerType),
              StructField("y", LongType)
            )
          )
        )
      )
    )
    assertEquals(summon[Encoder[Foo]].schema, expected)
  }

  test("encode dataset of case class Foo(x: String)") {
    case class Foo(x: String)
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val dataset = Seq(Foo("hello"), Foo("world")).toDS()
    dataset.show()
    spark.stop()
  }
