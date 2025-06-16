package scala3encoders

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types.*
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder

class DerivationTests extends munit.FunSuite:
  test("derive encoder of case class C(x: Int, y: Long)") {
    val encoder = summon[Encoder[C]].asInstanceOf[AgnosticEncoder[C]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("x", IntegerType, nullable = false),
          StructField("y", LongType, nullable = false)
        )
      )
    )
  }

  test("derive encoder of case class with Scala BigDecimal and BigInt") {
    val encoder = summon[Encoder[G]]
      .asInstanceOf[AgnosticEncoder[G]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("x", DecimalType(38, 18), nullable = false),
          StructField("y", DecimalType(38, 0), nullable = false)
        )
      )
    )
  }

  test("derive encoder of case class with Java BigDecimal and BigInteger") {
    val encoder = summon[Encoder[H]]
      .asInstanceOf[AgnosticEncoder[H]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("x", DecimalType(38, 18), nullable = false),
          StructField("y", DecimalType(38, 0), nullable = false)
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
      encoderAdv
        .asInstanceOf[AgnosticEncoder[Seq[D]]]
        .dataType
        .asInstanceOf[ArrayType]
        .elementType,
      summon[Encoder[D]].asInstanceOf[AgnosticEncoder[Seq[D]]].dataType
    )
  }

  test("derive encoder of Set") {
    val encoderBase = summon[Encoder[Set[Int]]]
    assertEquals(
      encoderBase.schema,
      StructType(
        Seq(
          StructField("value", ArrayType(IntegerType, false), nullable = true)
        )
      )
    )

    val encoderAdv = summon[Encoder[Set[D]]]
    assertEquals(
      encoderAdv
        .asInstanceOf[AgnosticEncoder[Set[D]]]
        .dataType
        .asInstanceOf[ArrayType]
        .elementType,
      summon[Encoder[D]].asInstanceOf[AgnosticEncoder[Set[D]]].dataType
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
      encoderAdv
        .asInstanceOf[AgnosticEncoder[Array[D]]]
        .dataType
        .asInstanceOf[ArrayType]
        .elementType,
      summon[Encoder[D]].asInstanceOf[AgnosticEncoder[Array[D]]].dataType
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
              false
            ),
            nullable = true
          )
        )
      )
    )

    val encoderAdv = summon[Encoder[Map[D, D]]]

    val k = encoderAdv
      .asInstanceOf[AgnosticEncoder[Map[D, D]]]
      .dataType
      .asInstanceOf[MapType]
    assertEquals(
      k.keyType,
      summon[Encoder[D]].asInstanceOf[AgnosticEncoder[D]].dataType
    )
    assertEquals(
      k.valueType,
      summon[Encoder[D]].asInstanceOf[AgnosticEncoder[D]].dataType
    )
  }
