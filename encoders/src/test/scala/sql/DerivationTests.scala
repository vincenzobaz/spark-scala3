package scala3encoders

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Literal, UpCast}
import org.apache.spark.sql.catalyst.expressions.objects.{NewInstance, StaticInvoke}
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

  test("derived encoder of case class C(x: Int, y: Long) should have deserializer") {
    val encoder = summon[Encoder[C]].asInstanceOf[ExpressionEncoder[C]]
    assertEquals(
      encoder.deserializer,
      NewInstance(
        cls = classOf[C],
        arguments = Vector(
          StaticInvoke(
            staticObject = classOf[Integer],
            dataType = ObjectType(classOf[Integer]),
            functionName = "valueOf",
            arguments = Vector(
              UpCast(
                child = UnresolvedAttribute(nameParts = List("x")),
                target = IntegerType,
                walkedTypePath = Nil
              )
            ),
            inputTypes = Nil,
            propagateNull = true,
            returnNullable = false,
            isDeterministic = true
          ),
          StaticInvoke(
            staticObject = classOf[java.lang.Long],
            dataType = ObjectType(classOf[java.lang.Long]),
            functionName = "valueOf",
            arguments = Vector(
              UpCast(
                child = UnresolvedAttribute(nameParts = List("y")),
                target = LongType,
                walkedTypePath = Nil
              )
            ),
            inputTypes = Nil,
            propagateNull = true,
            returnNullable = false,
            isDeterministic = true
          )
        ),
        inputTypes = Nil,
        propagateNull = false,
        dataType = ObjectType(classOf[C]),
        outerPointer = None
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
