package scala3encoders

import scala.reflect.ClassTag
import scala.reflect.Enum
import scala.compiletime.{constValue, erasedValue, summonInline}

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInt}
import java.time.{Duration, Instant, LocalDate, Period}
import org.apache.spark.unsafe.types.{CalendarInterval, VariantVal}
import org.apache.spark.sql.types.Decimal
import scala.deriving.Mirror
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.EncoderField
import org.apache.spark.sql.types.MetadataBuilder

// Primitive encoders
given AgnosticEncoder[Boolean] = AgnosticEncoders.PrimitiveBooleanEncoder
given AgnosticEncoder[Byte] = AgnosticEncoders.PrimitiveByteEncoder
given AgnosticEncoder[Short] = AgnosticEncoders.PrimitiveShortEncoder
given AgnosticEncoder[Int] = AgnosticEncoders.PrimitiveIntEncoder
given AgnosticEncoder[Long] = AgnosticEncoders.PrimitiveLongEncoder
given AgnosticEncoder[Float] = AgnosticEncoders.PrimitiveFloatEncoder
given AgnosticEncoder[Double] = AgnosticEncoders.PrimitiveDoubleEncoder

// TODO
// Primitive wrapper encoders.
/*
given AgnosticEncoder[java.lang.Boolean] = AgnosticEncoders.BoxedBooleanEncoder
given AgnosticEncoder[java.lang.Byte] = AgnosticEncoders.BoxedByteEncoder
given AgnosticEncoder[java.lang.Short] = AgnosticEncoders.BoxedShortEncoder
given AgnosticEncoder[java.lang.Integer] = AgnosticEncoders.BoxedIntEncoder
given AgnosticEncoder[java.lang.Long] = AgnosticEncoders.BoxedLongEncoder
given AgnosticEncoder[java.lang.Float] = AgnosticEncoders.BoxedFloatEncoder
given AgnosticEncoder[java.lang.Double] = AgnosticEncoders.BoxedDoubleEncoder
 */

// Nullable leaf encoders
given AgnosticEncoder[String] = AgnosticEncoders.StringEncoder
given AgnosticEncoder[java.lang.Void] = AgnosticEncoders.NullEncoder
given AgnosticEncoder[Array[Byte]] = AgnosticEncoders.BinaryEncoder
given AgnosticEncoder[BigInt] = AgnosticEncoders.ScalaBigIntEncoder
given AgnosticEncoder[JBigInt] = AgnosticEncoders.JavaBigIntEncoder
given AgnosticEncoder[CalendarInterval] =
  AgnosticEncoders.CalendarIntervalEncoder
given AgnosticEncoder[Duration] = AgnosticEncoders.DayTimeIntervalEncoder
given AgnosticEncoder[Period] = AgnosticEncoders.YearMonthIntervalEncoder
given AgnosticEncoder[VariantVal] = AgnosticEncoders.VariantEncoder

given AgnosticEncoder[java.sql.Date] = AgnosticEncoders.STRICT_DATE_ENCODER
given AgnosticEncoder[LocalDate] = AgnosticEncoders.STRICT_LOCAL_DATE_ENCODER
given AgnosticEncoder[java.sql.Timestamp] =
  AgnosticEncoders.STRICT_TIMESTAMP_ENCODER
given AgnosticEncoder[Instant] = AgnosticEncoders.STRICT_INSTANT_ENCODER

given AgnosticEncoder[Decimal] = AgnosticEncoders.DEFAULT_SPARK_DECIMAL_ENCODER

given AgnosticEncoder[BigDecimal] =
  AgnosticEncoders.DEFAULT_SCALA_DECIMAL_ENCODER
given AgnosticEncoder[JBigDecimal] =
  AgnosticEncoders.DEFAULT_JAVA_DECIMAL_ENCODER

given [O: AgnosticEncoder]: AgnosticEncoder[Option[O]] =
  AgnosticEncoders.OptionEncoder(summon[AgnosticEncoder[O]])
given [O: AgnosticEncoder]: AgnosticEncoder[Array[O]] =
  AgnosticEncoders.ArrayEncoder(summon[AgnosticEncoder[O]], false)

given [C[_], E](using
    ev: C[E] <:< Iterable[E],
    classTag: ClassTag[C[E]],
    element: AgnosticEncoder[E]
): AgnosticEncoder[C[E]] =
  AgnosticEncoders.IterableEncoder(classTag, element, false, false)

given [M[_, _], K, V](using
    ev: M[K, V] <:< Map[K, V],
    classTag: ClassTag[M[K, V]],
    key: AgnosticEncoder[K],
    value: AgnosticEncoder[V]
): AgnosticEncoder[M[K, V]] =
  AgnosticEncoders.MapEncoder(classTag, key, value, false)

private inline def summonAll[T <: Tuple, U <: Tuple]: List[EncoderField] =
  inline (erasedValue[T], erasedValue[U]) match
    // to support case classes with larger number of parameters we have to
    // unpack the members in bigger chunks to not evoke a compiler stackoverflow here
    // also Xmax-inlines may not need to be adapted
    case _: (
            t1 *: t2 *: t3 *: t4 *: t5 *: t6 *: t7 *: t8 *: t9 *: t10 *:
              t11 *: t12 *: t13 *: t14 *: t15 *: t16 *: ts,
            u1 *: u2 *: u3 *: u4 *: u5 *: u6 *: u7 *: u8 *: u9 *: u10 *:
              u11 *: u12 *: u13 *: u14 *: u15 *: u16 *: us
        ) =>
      List(
        (constValue[t1].toString, summonInline[Serializer[u1]]),
        (constValue[t2].toString, summonInline[Serializer[u2]]),
        (constValue[t3].toString, summonInline[Serializer[u3]]),
        (constValue[t4].toString, summonInline[Serializer[u4]]),
        (constValue[t5].toString, summonInline[Serializer[u5]]),
        (constValue[t6].toString, summonInline[Serializer[u6]]),
        (constValue[t7].toString, summonInline[Serializer[u7]]),
        (constValue[t8].toString, summonInline[Serializer[u8]]),
        (constValue[t9].toString, summonInline[Serializer[u9]]),
        (constValue[t10].toString, summonInline[Serializer[u10]]),
        (constValue[t11].toString, summonInline[Serializer[u11]]),
        (constValue[t12].toString, summonInline[Serializer[u12]]),
        (constValue[t13].toString, summonInline[Serializer[u13]]),
        (constValue[t14].toString, summonInline[Serializer[u14]]),
        (constValue[t15].toString, summonInline[Serializer[u15]]),
        (constValue[t16].toString, summonInline[Serializer[u16]])
      )
        ::: summonAll[ts, us]
    case _: ((t *: ts), (u *: us)) =>
      EncoderField(
        constValue[t].toString,
        summonInline[AgnosticEncoder[u]],
        false,
        MetadataBuilder()
          .build() // TODO: Is this ok? what about the two other fields
      ) :: summonAll[ts, us]
    case _ => Nil

inline given [T](using
    mirror: Mirror.ProductOf[T],
    classTag: ClassTag[T]
): AgnosticEncoder[T] =
  AgnosticEncoders.ProductEncoder(
    classTag,
    summonAll[mirror.MirroredElemLabels, mirror.MirroredElemTypes],
    None
  )

// TODO: Enums