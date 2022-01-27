package scala3encoders

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

case class A()
case class B(x: String)
case class C(x: Int, y: Long)
case class D(x: String, y: B)

class EncoderDerivationSpec extends munit.FunSuite with SparkSqlTesting:
  private val dSchema = 
    StructType(
      Seq(
        StructField("x", StringType),
        StructField("y", StructType(Seq(StructField("x", StringType))))
      )
    )

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

  test("derive encoder of Seq") {
    val encoderBase = summon[Encoder[Seq[Int]]]
    assertEquals(
      encoderBase.schema,
      StructType(
        Seq(
          StructField("value", ArrayType(IntegerType, true), true)
        )
      )
    )

    val encoderAdv = summon[Encoder[Seq[D]]]
    assertEquals(
      encoderAdv.schema,
      StructType(
        Seq(
          StructField("value", ArrayType(dSchema, true), true)
        )
      )
    )

  }

  test("derive encoder of Set") {
    val encoderBase = summon[Encoder[Set[Int]]]
    assertEquals(
      encoderBase.schema,
      StructType(
        Seq(
          StructField("value", ArrayType(IntegerType, true), true)
        )
      )
    )

    val encoderAdv = summon[Encoder[Set[D]]]
    assertEquals(
      encoderAdv.schema,
      StructType(
        Seq(
          StructField("value", ArrayType(dSchema, true), true)
        )
      )
    )
  }

  test("derive encoder of Array") {
    val encoderBase = summon[Encoder[Array[Int]]]
    assertEquals(
      encoderBase.schema,
      StructType(
        Seq(
          StructField("value", ArrayType(IntegerType, false), true)
        )
      )
    )

    val encoderAdv = summon[Encoder[Array[D]]]
    assertEquals(
      encoderAdv.schema,
      StructType(
        Seq(
          StructField("value", ArrayType(dSchema, true), true)
        )
      )
    )
  }

  test("derive encoder of Map") {
    val encoderBase = summon[Encoder[Map[Int, String]]]
    assertEquals(
      encoderBase.schema,
      StructType(
        Seq(
          StructField(
            "value",
            MapType(
              IntegerType,
              StringType,
              true
            )
          )
        )
      )
    )

    val encoderAdv = summon[Encoder[Map[D, D]]]
    assertEquals(
      encoderAdv.schema,
      StructType(
        Seq(
          StructField(
            "value",
            MapType(
              dSchema,
              dSchema,
              true
            )
          )
        )
      )
    )
  }
