package scala3encoders

import scala.reflect.ClassTag
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
import scala.concurrent.duration.FiniteDuration
import org.apache.spark.sql.catalyst.encoders.Codec
import scala.jdk.javaapi.DurationConverters

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
  val enc = summon[AgnosticEncoder[O]]
  AgnosticEncoders.ArrayEncoder(enc, containsNull = enc.nullable)

given [C[_], E](using
    ev: C[E] <:< Iterable[E],
    classTag: ClassTag[C[E]],
    element: AgnosticEncoder[E]
): AgnosticEncoder[C[E]] =
  AgnosticEncoders.IterableEncoder(classTag, element, containsNull = element.nullable, lenientSerialization = false)

given [M[_, _], K, V](using
    ev: M[K, V] <:< Map[K, V],
    classTag: ClassTag[M[K, V]],
    key: AgnosticEncoder[K],
    value: AgnosticEncoder[V]
): AgnosticEncoder[M[K, V]] =
  AgnosticEncoders.MapEncoder(classTag, key, value, valueContainsNull = value.nullable)

private inline def summonAll[T <: Tuple, U <: Tuple]: List[EncoderField] =
  def encoder[T](label: String, enc: AgnosticEncoder[T]) =
    EncoderField(
      label,
      enc,
      nullable = enc.nullable,
      MetadataBuilder()
        .build() // TODO: Is this ok? what about the two other fields
    )

  inline (erasedValue[T], erasedValue[U]) match
    // to support case classes with larger number of parameters we have to
    // unpack the members in bigger chunks to not evoke a compiler stackoverflow here
    // also Xmax-inlines may not need to be adapted
    case _: (
            t1 *: t2 *: t3 *: t4 *: t5 *: t6 *: t7 *: t8 *: t9 *: t10 *: t11 *:
              t12 *: t13 *: t14 *: t15 *: t16 *: ts,
            u1 *: u2 *: u3 *: u4 *: u5 *: u6 *: u7 *: u8 *: u9 *: u10 *: u11 *:
              u12 *: u13 *: u14 *: u15 *: u16 *: us
        ) =>
      List(
        encoder(constValue[t1].toString, summonInline[AgnosticEncoder[u1]]),
        encoder(constValue[t2].toString, summonInline[AgnosticEncoder[u2]]),
        encoder(constValue[t3].toString, summonInline[AgnosticEncoder[u3]]),
        encoder(constValue[t4].toString, summonInline[AgnosticEncoder[u4]]),
        encoder(constValue[t5].toString, summonInline[AgnosticEncoder[u5]]),
        encoder(constValue[t6].toString, summonInline[AgnosticEncoder[u6]]),
        encoder(constValue[t7].toString, summonInline[AgnosticEncoder[u7]]),
        encoder(constValue[t8].toString, summonInline[AgnosticEncoder[u8]]),
        encoder(constValue[t9].toString, summonInline[AgnosticEncoder[u9]]),
        encoder(constValue[t10].toString, summonInline[AgnosticEncoder[u10]]),
        encoder(constValue[t11].toString, summonInline[AgnosticEncoder[u11]]),
        encoder(constValue[t12].toString, summonInline[AgnosticEncoder[u12]]),
        encoder(constValue[t13].toString, summonInline[AgnosticEncoder[u13]]),
        encoder(constValue[t14].toString, summonInline[AgnosticEncoder[u14]]),
        encoder(constValue[t15].toString, summonInline[AgnosticEncoder[u15]]),
        encoder(constValue[t16].toString, summonInline[AgnosticEncoder[u16]])
      )
        ::: summonAll[ts, us]
    case _: ((t *: ts), (u *: us)) =>
      encoder(
        constValue[t].toString,
        summonInline[AgnosticEncoder[u]]
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

given AgnosticEncoder[FiniteDuration] =
  AgnosticEncoders.TransformingEncoder(
    summon[ClassTag[FiniteDuration]],
    summon[AgnosticEncoder[Duration]],
    () =>
      new Codec[FiniteDuration, Duration] {
        override def decode(out: Duration): FiniteDuration =
          DurationConverters.toScala(out)
        override def encode(in: FiniteDuration): Duration =
          DurationConverters.toJava(in)
      }
  )

// TODO: Enums
