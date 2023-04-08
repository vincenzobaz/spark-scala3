package org.apache.spark.sql.expressions

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.DataType

/// hack: since `SparkUserDefinedFunction` is private in spark, we define an
/// `Exporter` object that is located in the same package as the `SparkUserDefinedFunction`
/// created here.
/// This is also used to get all the needed encoders and decoders that are generated
/// by `scala3encoders` package.
object Exporter:
  def createUdf(
      f: AnyRef,
      dataType: DataType,
      inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil,
      outputEncoder: Option[ExpressionEncoder[_]] = None,
      name: Option[String] = None,
      nullable: Boolean = true,
      deterministic: Boolean = true
  ): UserDefinedFunction =
    SparkUserDefinedFunction(
      f,
      dataType,
      inputEncoders,
      outputEncoder,
      name,
      nullable,
      deterministic
    )
