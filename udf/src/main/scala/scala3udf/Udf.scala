package scala3udf

import scala.reflect.ClassTag

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.compiletime.{summonInline, erasedValue}
import scala.deriving.Mirror
import scala.quoted.*

import scala3encoders.derivation.Deserializer

/// For scala3: instead of calling spark.udf(...) call Udf(...)
/// Udf wraps the `UserDefinedFunction`. In order to register that
/// function with the spark function registry (needed in `spark.sql` statements written as strings)
/// call the `register` function.
///
/// @note the register function is only successful when the underlying callback
/// has been defined and created on package level. Otherwise the serialization/deserialization
/// in spark will fail (with Scala3). This is checked inside register and will throw.
/// It is recommended _not_ to use `spark.sql` statements and use the idiomatic `select`, `filter`, `map` etc
/// spark interfaces.
final case class Udf private (udf: UserDefinedFunction, f: AnyRef):
  @scala.annotation.varargs
  def apply(exprs: Column*): Column = udf.apply(exprs: _*)

  def register(name: String = "")(using
      spark: SparkSession
  ): UserDefinedFunction =
    register_with(spark, name)

  def register_with(spark: SparkSession, name: String): UserDefinedFunction =
    Udf.internal_register(spark, name, f, udf)

object Udf:
  /// helper macro to simplify registering a lambda function variable using its name
  /// instead of writing
  /// `my_fun.register("my_fun"); my_other_fun.register("my_other_fun"); `
  /// you can also write
  /// `Udf.register(my_fun, my_other_fun, etc...)`
  inline def register(inline udfs: Udf*)(using spark: SparkSession) =
    ${ register_udf_impl('udfs, 'spark) }

  /// same as `register` but with explicit `SparkSession` parameter
  inline def register_with(spark: SparkSession, inline udfs: Udf*) =
    ${ register_udf_impl('udfs, 'spark) }

  private inline def var_name(object_name: String): String =
    object_name.substring(object_name.lastIndexOf(".") + 1)

  private def register_udf_impl(
      udfs: Expr[Seq[Udf]],
      spark: Expr[SparkSession]
  )(using Quotes): Expr[Unit] =
    import quotes.reflect.report
    val names = udfs match
      case Varargs(udfExprs) => // udfExprs: Seq[Expr[Udf]]
        udfExprs.map { udf =>
          var_name(udf.show)
        }
      case _ =>
        report.errorAndAbort(
          "Expected explicit varargs sequence. " +
            "Notation `args*` is not supported.",
          udfs
        )

    '{
      ${ udfs }.zipWithIndex.map((udf, idx) =>
        udf.register_with(${ spark }, ${ Expr(names) }(idx))
      )
    }

  /// wrapper for `UdfHelper.createUdf` written in Java to create
  /// the otherwise private `SparkUserDefinedFunction`.
  /// This is also used to get all the needed encoders and decoders generated
  /// by `scala3encoders` package.
  private def create_udf(
      f: AnyRef,
      dataType: DataType,
      inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil,
      outputEncoder: Option[ExpressionEncoder[_]] = None,
      name: Option[String] = None,
      nullable: Boolean = true,
      deterministic: Boolean = true
  ): Udf =
    val udf = UdfHelper
      .createUdf(
        f,
        dataType,
        inputEncoders,
        outputEncoder,
        name,
        nullable,
        deterministic
      )
      .asInstanceOf[UserDefinedFunction]
    Udf(udf, f)

  private def internal_register(
      spark: SparkSession,
      register_name: String,
      f: Any,
      udf: UserDefinedFunction
  ): UserDefinedFunction =
    if (register_name.isEmpty()) then
      throw IllegalArgumentException(
        "must provide a name when providing a session"
      )

    if (!f.getClass().getName().contains("$package$")) then
      throw IllegalArgumentException(
        "provided function has to be moved to package level!"
      )

    spark.udf.register(register_name, udf)

  private inline def internal_summon_seq[T <: Tuple]
      : List[Option[ExpressionEncoder[_]]] =
    inline erasedValue[T] match
      case _: (t *: ts) =>
        Some(summonInline[ExpressionEncoder[t]]) :: internal_summon_seq[ts]
      case _ => Nil

  private inline def summon_seq[T <: Tuple](using
      mirror: Mirror.ProductOf[T],
      classTag: ClassTag[T]
  ): Seq[Option[ExpressionEncoder[_]]] =
    internal_summon_seq[mirror.MirroredElemTypes]

  // the apply part has been auto generated in `Gen.scala`

  def apply[R](
      f: Function0[R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      Seq(),
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, R](
      f: Function1[T1, R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      Seq(Some(summon[ExpressionEncoder[T1]])),
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, T2, R](
      f: Function2[T1, T2, R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[(T1, T2)],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, T2, T3, R](
      f: Function3[T1, T2, T3, R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[(T1, T2, T3)],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, T2, T3, T4, R](
      f: Function4[T1, T2, T3, T4, R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[(T1, T2, T3, T4)],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, T2, T3, T4, T5, R](
      f: Function5[T1, T2, T3, T4, T5, R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[(T1, T2, T3, T4, T5)],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, T2, T3, T4, T5, T6, R](
      f: Function6[T1, T2, T3, T4, T5, T6, R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[(T1, T2, T3, T4, T5, T6)],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, T2, T3, T4, T5, T6, T7, R](
      f: Function7[T1, T2, T3, T4, T5, T6, T7, R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[(T1, T2, T3, T4, T5, T6, T7)],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, T2, T3, T4, T5, T6, T7, T8, R](
      f: Function8[T1, T2, T3, T4, T5, T6, T7, T8, R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[(T1, T2, T3, T4, T5, T6, T7, T8)],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](
      f: Function9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[(T1, T2, T3, T4, T5, T6, T7, T8, T9)],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R](
      f: Function10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9],
      et10: ExpressionEncoder[T10]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R](
      f: Function11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9],
      et10: ExpressionEncoder[T10],
      et11: ExpressionEncoder[T11]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, R](
      f: Function12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9],
      et10: ExpressionEncoder[T10],
      et11: ExpressionEncoder[T11],
      et12: ExpressionEncoder[T12]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, R](
      f: Function13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, R]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9],
      et10: ExpressionEncoder[T10],
      et11: ExpressionEncoder[T11],
      et12: ExpressionEncoder[T12],
      et13: ExpressionEncoder[T13]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, R](
      f: Function14[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        R
      ]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9],
      et10: ExpressionEncoder[T10],
      et11: ExpressionEncoder[T11],
      et12: ExpressionEncoder[T12],
      et13: ExpressionEncoder[T13],
      et14: ExpressionEncoder[T14]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[
      T1,
      T2,
      T3,
      T4,
      T5,
      T6,
      T7,
      T8,
      T9,
      T10,
      T11,
      T12,
      T13,
      T14,
      T15,
      R
  ](
      f: Function15[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        R
      ]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9],
      et10: ExpressionEncoder[T10],
      et11: ExpressionEncoder[T11],
      et12: ExpressionEncoder[T12],
      et13: ExpressionEncoder[T13],
      et14: ExpressionEncoder[T14],
      et15: ExpressionEncoder[T15]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[
        (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)
      ],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[
      T1,
      T2,
      T3,
      T4,
      T5,
      T6,
      T7,
      T8,
      T9,
      T10,
      T11,
      T12,
      T13,
      T14,
      T15,
      T16,
      R
  ](
      f: Function16[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        R
      ]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9],
      et10: ExpressionEncoder[T10],
      et11: ExpressionEncoder[T11],
      et12: ExpressionEncoder[T12],
      et13: ExpressionEncoder[T13],
      et14: ExpressionEncoder[T14],
      et15: ExpressionEncoder[T15],
      et16: ExpressionEncoder[T16]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[
        (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16)
      ],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[
      T1,
      T2,
      T3,
      T4,
      T5,
      T6,
      T7,
      T8,
      T9,
      T10,
      T11,
      T12,
      T13,
      T14,
      T15,
      T16,
      T17,
      R
  ](
      f: Function17[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        R
      ]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9],
      et10: ExpressionEncoder[T10],
      et11: ExpressionEncoder[T11],
      et12: ExpressionEncoder[T12],
      et13: ExpressionEncoder[T13],
      et14: ExpressionEncoder[T14],
      et15: ExpressionEncoder[T15],
      et16: ExpressionEncoder[T16],
      et17: ExpressionEncoder[T17]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[
        (
            T1,
            T2,
            T3,
            T4,
            T5,
            T6,
            T7,
            T8,
            T9,
            T10,
            T11,
            T12,
            T13,
            T14,
            T15,
            T16,
            T17
        )
      ],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[
      T1,
      T2,
      T3,
      T4,
      T5,
      T6,
      T7,
      T8,
      T9,
      T10,
      T11,
      T12,
      T13,
      T14,
      T15,
      T16,
      T17,
      T18,
      R
  ](
      f: Function18[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        R
      ]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9],
      et10: ExpressionEncoder[T10],
      et11: ExpressionEncoder[T11],
      et12: ExpressionEncoder[T12],
      et13: ExpressionEncoder[T13],
      et14: ExpressionEncoder[T14],
      et15: ExpressionEncoder[T15],
      et16: ExpressionEncoder[T16],
      et17: ExpressionEncoder[T17],
      et18: ExpressionEncoder[T18]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[
        (
            T1,
            T2,
            T3,
            T4,
            T5,
            T6,
            T7,
            T8,
            T9,
            T10,
            T11,
            T12,
            T13,
            T14,
            T15,
            T16,
            T17,
            T18
        )
      ],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[
      T1,
      T2,
      T3,
      T4,
      T5,
      T6,
      T7,
      T8,
      T9,
      T10,
      T11,
      T12,
      T13,
      T14,
      T15,
      T16,
      T17,
      T18,
      T19,
      R
  ](
      f: Function19[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        T19,
        R
      ]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9],
      et10: ExpressionEncoder[T10],
      et11: ExpressionEncoder[T11],
      et12: ExpressionEncoder[T12],
      et13: ExpressionEncoder[T13],
      et14: ExpressionEncoder[T14],
      et15: ExpressionEncoder[T15],
      et16: ExpressionEncoder[T16],
      et17: ExpressionEncoder[T17],
      et18: ExpressionEncoder[T18],
      et19: ExpressionEncoder[T19]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[
        (
            T1,
            T2,
            T3,
            T4,
            T5,
            T6,
            T7,
            T8,
            T9,
            T10,
            T11,
            T12,
            T13,
            T14,
            T15,
            T16,
            T17,
            T18,
            T19
        )
      ],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[
      T1,
      T2,
      T3,
      T4,
      T5,
      T6,
      T7,
      T8,
      T9,
      T10,
      T11,
      T12,
      T13,
      T14,
      T15,
      T16,
      T17,
      T18,
      T19,
      T20,
      R
  ](
      f: Function20[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        T19,
        T20,
        R
      ]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9],
      et10: ExpressionEncoder[T10],
      et11: ExpressionEncoder[T11],
      et12: ExpressionEncoder[T12],
      et13: ExpressionEncoder[T13],
      et14: ExpressionEncoder[T14],
      et15: ExpressionEncoder[T15],
      et16: ExpressionEncoder[T16],
      et17: ExpressionEncoder[T17],
      et18: ExpressionEncoder[T18],
      et19: ExpressionEncoder[T19],
      et20: ExpressionEncoder[T20]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[
        (
            T1,
            T2,
            T3,
            T4,
            T5,
            T6,
            T7,
            T8,
            T9,
            T10,
            T11,
            T12,
            T13,
            T14,
            T15,
            T16,
            T17,
            T18,
            T19,
            T20
        )
      ],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[
      T1,
      T2,
      T3,
      T4,
      T5,
      T6,
      T7,
      T8,
      T9,
      T10,
      T11,
      T12,
      T13,
      T14,
      T15,
      T16,
      T17,
      T18,
      T19,
      T20,
      T21,
      R
  ](
      f: Function21[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        T19,
        T20,
        T21,
        R
      ]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9],
      et10: ExpressionEncoder[T10],
      et11: ExpressionEncoder[T11],
      et12: ExpressionEncoder[T12],
      et13: ExpressionEncoder[T13],
      et14: ExpressionEncoder[T14],
      et15: ExpressionEncoder[T15],
      et16: ExpressionEncoder[T16],
      et17: ExpressionEncoder[T17],
      et18: ExpressionEncoder[T18],
      et19: ExpressionEncoder[T19],
      et20: ExpressionEncoder[T20],
      et21: ExpressionEncoder[T21]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[
        (
            T1,
            T2,
            T3,
            T4,
            T5,
            T6,
            T7,
            T8,
            T9,
            T10,
            T11,
            T12,
            T13,
            T14,
            T15,
            T16,
            T17,
            T18,
            T19,
            T20,
            T21
        )
      ],
      Some(summon[ExpressionEncoder[R]]),
      None
    )

  def apply[
      T1,
      T2,
      T3,
      T4,
      T5,
      T6,
      T7,
      T8,
      T9,
      T10,
      T11,
      T12,
      T13,
      T14,
      T15,
      T16,
      T17,
      T18,
      T19,
      T20,
      T21,
      T22,
      R
  ](
      f: Function22[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        T19,
        T20,
        T21,
        T22,
        R
      ]
  )(using
      er: ExpressionEncoder[R],
      dr: Deserializer[R],
      et1: ExpressionEncoder[T1],
      et2: ExpressionEncoder[T2],
      et3: ExpressionEncoder[T3],
      et4: ExpressionEncoder[T4],
      et5: ExpressionEncoder[T5],
      et6: ExpressionEncoder[T6],
      et7: ExpressionEncoder[T7],
      et8: ExpressionEncoder[T8],
      et9: ExpressionEncoder[T9],
      et10: ExpressionEncoder[T10],
      et11: ExpressionEncoder[T11],
      et12: ExpressionEncoder[T12],
      et13: ExpressionEncoder[T13],
      et14: ExpressionEncoder[T14],
      et15: ExpressionEncoder[T15],
      et16: ExpressionEncoder[T16],
      et17: ExpressionEncoder[T17],
      et18: ExpressionEncoder[T18],
      et19: ExpressionEncoder[T19],
      et20: ExpressionEncoder[T20],
      et21: ExpressionEncoder[T21],
      et22: ExpressionEncoder[T22]
  ): Udf =
    create_udf(
      f,
      dr.inputType,
      summon_seq[
        (
            T1,
            T2,
            T3,
            T4,
            T5,
            T6,
            T7,
            T8,
            T9,
            T10,
            T11,
            T12,
            T13,
            T14,
            T15,
            T16,
            T17,
            T18,
            T19,
            T20,
            T21,
            T22
        )
      ],
      Some(summon[ExpressionEncoder[R]]),
      None
    )
