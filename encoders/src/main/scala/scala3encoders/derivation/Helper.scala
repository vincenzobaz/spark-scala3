package org.apache.spark.sql.helper

import org.apache.spark.sql.catalyst.expressions.{
  CheckOverflow,
  Expression,
  UpCast
}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.DeserializerBuildHelper.expressionWithNullSafety
import org.apache.spark.sql.catalyst.WalkedTypePath
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

// This is copied from spark to support older versions of Spark and 3.5.0 -
// it was part of ScalaReflection and was moved to EncoderUtils in 3.5.0
object Helper {
  private val nullOnOverflow = !SQLConf.get.ansiEnabled

  val typeBoxedJavaMapping: Map[Class[?], Class[?]] = Map[Class[?], Class[?]](
    classOf[Boolean] -> classOf[java.lang.Boolean],
    classOf[Byte] -> classOf[java.lang.Byte],
    classOf[Short] -> classOf[java.lang.Short],
    classOf[Int] -> classOf[java.lang.Integer],
    classOf[Long] -> classOf[java.lang.Long],
    classOf[Float] -> classOf[java.lang.Float],
    classOf[Double] -> classOf[java.lang.Double]
  )

  def createSerializerForBigInteger(inputObject: Expression): Expression = {
    CheckOverflow(
      StaticInvoke(
        Decimal.getClass,
        DecimalType.BigIntDecimal,
        "apply",
        inputObject :: Nil,
        returnNullable = false
      ),
      DecimalType.BigIntDecimal,
      nullOnOverflow
    )
  }

  def createSerializerForBigDecimal(inputObject: Expression): Expression = {
    CheckOverflow(
      StaticInvoke(
        Decimal.getClass,
        DecimalType(38, 18),
        "apply",
        inputObject :: Nil,
        returnNullable = false
      ),
      DecimalType(38, 18),
      nullOnOverflow
    )
  }

  private def upCastToExpectedType(
      expr: Expression,
      expected: DataType,
      walkedTypePath: WalkedTypePath
  ): Expression = expected match {
    case _: StructType  => expr
    case _: ArrayType   => expr
    case _: MapType     => expr
    case _: DecimalType =>
      // For Scala/Java `BigDecimal`, we accept decimal types of any valid precision/scale.
      // Here we use the `DecimalType` object to indicate it.
      UpCast(expr, DecimalType, walkedTypePath.getPaths)
    case _ => UpCast(expr, expected, walkedTypePath.getPaths)
  }

  def deserializerForWithNullSafetyAndUpcast(
      expr: Expression,
      dataType: DataType,
      nullable: Boolean,
      walkedTypePath: WalkedTypePath,
      funcForCreatingDeserializer: Expression => Expression
  ): Expression = {
    val casted = upCastToExpectedType(expr, dataType, walkedTypePath)
    expressionWithNullSafety(
      funcForCreatingDeserializer(casted),
      nullable,
      walkedTypePath
    )
  }
}
