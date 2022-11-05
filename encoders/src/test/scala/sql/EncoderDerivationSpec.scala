package scala3encoders

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types.*
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import scala3encoders.derivation.decimalType

case class A()
case class B(x: String)
case class C(x: Int, y: Long)
case class D(x: String, y: B)
case class E(x: String, y: BigInt)

class EncoderDerivationSpec extends munit.FunSuite with SparkSqlTesting:
  test("derive encoder of case class A()") {
    val encoder = summon[Encoder[A]]
    assertEquals(encoder.schema, StructType(Seq.empty))
    
    val input = Seq(A(), A())
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test("derive encoder of case class B(x: String)") {
    val encoder = summon[Encoder[B]]
    assertEquals(
      encoder.schema,
      StructType(Seq(StructField("x", StringType)))
    )

    val input = Seq(B("hello"), B("world"))
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test("derive encoder of case class C(x: Int, y: Long)") {
    val encoder = summon[Encoder[C]].asInstanceOf[ExpressionEncoder[C]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("x", IntegerType),
          StructField("y", LongType)
        )
      )
    )

    val input = Seq(C(42, -9_223_372_036_854_775_808L), C(0, 9_223_372_036_854_775_807L))
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test("derive encoder of case class D(x: String, y: B) where B is case class B(x: String)") {
    val encoder = summon[Encoder[D]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("x", StringType),
          StructField("y", StructType(Seq(StructField("x", StringType))))
        )
      )
    )

    val input = Seq(D("Hello", B("World")), D("Bye", B("Universe")))
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test("derive encoder of case class E(x: String, y: BigInt) where y fits into Long") {
    import derivation.Deserializer.bigInt.long.given
    import derivation.Serializer.bigInt.long.given
    val encoder = summon[Encoder[E]].asInstanceOf[ExpressionEncoder[E]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("x", StringType),
          StructField("y", LongType)
        )
      )
    )

    val input = Seq(E("hello", -9_223_372_036_854_775_808L), E("world", 9_223_372_036_854_775_807L))
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test("derive encoder of case class E(x: String, y: BigInt) where y doesn't fit into Long") {
    import derivation.Deserializer.bigInt.decimal.given
    import derivation.Serializer.bigInt.decimal.given
    val encoder = summon[Encoder[E]].asInstanceOf[ExpressionEncoder[E]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("x", StringType),
          StructField("y", decimalType)
        )
      )
    )

    val input = Seq(E("hello", BigInt("1000000000000000000000000")), E("world", BigInt("2000000000000000000000000")))
    assertEquals(input.toDS.collect.toSeq, input)
  }
