package scala3encoders

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types.*

class DerivationTests extends munit.FunSuite:
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
  }

  test("derive encoder of case class with Scala BigDecimal and BigInt") {
    val encoder = summon[Encoder[G]]
      .asInstanceOf[ExpressionEncoder[G]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("x", DecimalType(38, 18)),
          StructField("y", DecimalType(38, 0))
        )
      )
    )
  }

  test("derive encoder of case class with Java BigDecimal and BigInteger") {
    val encoder = summon[Encoder[H]]
      .asInstanceOf[ExpressionEncoder[H]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("x", DecimalType(38, 18)),
          StructField("y", DecimalType(38, 0))
        )
      )
    )
  }

  test("derive encoder of Seq") {
    val encoderBase = summon[Encoder[Seq[Int]]]
    assertEquals(
      encoderBase.schema,
      StructType(
        Seq(
          StructField("value", ArrayType(IntegerType, false), true)
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
          StructField("value", ArrayType(IntegerType, false), true)
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
