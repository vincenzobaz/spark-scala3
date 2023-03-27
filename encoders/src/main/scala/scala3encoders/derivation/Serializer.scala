package scala3encoders.derivation

import scala.compiletime.{constValue, summonInline, erasedValue}
import scala.deriving.Mirror
import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.expressions.{Expression, KnownNotNull}
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.SerializerBuildHelper.*
import org.apache.spark.sql.types.*
import org.apache.spark.sql.catalyst.expressions.objects.UnwrapOption
import org.apache.spark.sql.catalyst.ScalaReflection

trait Serializer[T]:
  def inputType: DataType
  def serialize(inputObject: Expression): Expression

object Serializer:
  // primitive types are not nullable
  private def isNullable(tpe: DataType): Boolean =
    !(tpe == BooleanType || tpe == ByteType || tpe == ShortType || tpe == IntegerType || tpe == LongType ||
      tpe == FloatType || tpe == DoubleType)

  inline given deriveOpt[T](using
      s: Serializer[T],
      ct: ClassTag[Option[T]]
  ): Serializer[Option[T]] =
    new Serializer[Option[T]]:
      override def inputType: DataType =
        ObjectType(ct.runtimeClass)
      override def serialize(inputObject: Expression): Expression =
        s.serialize(UnwrapOption(s.inputType, inputObject))

  given Serializer[Int] with
    def inputType: DataType = IntegerType
    def serialize(inputObject: Expression): Expression = inputObject

  given Serializer[java.lang.Integer] with
    def inputType: DataType = IntegerType
    def serialize(inputObject: Expression): Expression = inputObject

  given Serializer[Long] with
    def inputType: DataType = LongType
    def serialize(inputObject: Expression): Expression = inputObject

  given Serializer[Double] with
    def inputType: DataType = DoubleType
    def serialize(inputObject: Expression): Expression = inputObject

  given Serializer[Float] with
    def inputType: DataType = FloatType
    def serialize(inputObject: Expression): Expression = inputObject

  given Serializer[Short] with
    def inputType: DataType = ShortType
    def serialize(inputObject: Expression): Expression = inputObject

  given Serializer[Byte] with
    def inputType: DataType = ByteType
    def serialize(inputObject: Expression): Expression = inputObject

  given Serializer[Boolean] with
    def inputType: DataType = BooleanType
    def serialize(inputObject: Expression): Expression = inputObject

  given Serializer[java.time.LocalDate] with
    def inputType: DataType = ObjectType(classOf[java.time.LocalDate])
    def serialize(inputObject: Expression): Expression =
      createSerializerForJavaLocalDate(
        inputObject
      )

  given Serializer[java.sql.Date] with
    def inputType: DataType = ObjectType(classOf[java.sql.Date])
    def serialize(inputObject: Expression): Expression =
      createSerializerForSqlDate(inputObject)

  given Serializer[java.time.Instant] with
    def inputType: DataType = ObjectType(classOf[java.time.Instant])
    def serialize(inputObject: Expression): Expression =
      createSerializerForJavaInstant(inputObject)

  given Serializer[java.sql.Timestamp] with
    def inputType: DataType = ObjectType(classOf[java.sql.Timestamp])
    def serialize(inputObject: Expression): Expression =
      createSerializerForSqlTimestamp(inputObject)

  given Serializer[java.time.Duration] with
    def inputType: DataType = ObjectType(classOf[java.time.Duration])
    def serialize(inputObject: Expression): Expression =
      createSerializerForJavaDuration(inputObject)

  given Serializer[java.time.Period] with
    def inputType: DataType = ObjectType(classOf[java.time.Period])
    def serialize(inputObject: Expression): Expression =
      createSerializerForJavaPeriod(inputObject)

  given Serializer[BigDecimal] with
    def inputType: DataType = ObjectType(classOf[BigDecimal])
    def serialize(inputObject: Expression): Expression =
      createSerializerForScalaBigDecimal(inputObject)

  given Serializer[java.math.BigInteger] with
    def inputType: DataType = ObjectType(classOf[java.math.BigInteger])
    def serialize(inputObject: Expression): Expression =
      createSerializerForJavaBigInteger(inputObject)

  given Serializer[scala.math.BigInt] with
    def inputType: DataType = ObjectType(classOf[scala.math.BigInt])
    def serialize(inputObject: Expression): Expression =
      createSerializerForScalaBigInt(inputObject)

  // TODO
  /*given Serializer[Enum[_]] with
    def inputType: DataType = ObjectType(classOf[Enum[_]])
    def serialize(inputObject: Expression): Expression =
        createSerializerForJavaEnum(inputObject)*/

  given Serializer[String] with
    def inputType: DataType = ObjectType(classOf[String])
    def serialize(inputObject: Expression): Expression =
      createSerializerForString(inputObject)

  given deriveSeq[F[_], T](using s: Serializer[T])(using
      F[T] <:< Seq[T]
  ): Serializer[F[T]] =
    new Serializer[F[T]]:
      override def inputType: DataType = ObjectType(classOf[Seq[T]])
      override def serialize(inputObject: Expression): Expression =
        s.inputType match
          case dt: ObjectType =>
            createSerializerForMapObjects(inputObject, dt, s.serialize(_))
          case dt =>
            createSerializerForGenericArray(
              inputObject,
              dt,
              isNullable(s.inputType)
            )

  inline given deriveArray[T: Serializer: ClassTag](using
      s: Serializer[T]
  ): Serializer[Array[T]] =
    new Serializer[Array[T]]:
      override def inputType: DataType = ObjectType(classOf[Array[T]])
      override def serialize(inputObject: Expression) =
        s.inputType match
          case dt: ObjectType =>
            createSerializerForMapObjects(inputObject, dt, s.serialize(_))
          case dt =>
            if (isNullable(dt)) then
              createSerializerForGenericArray(inputObject, dt, nullable = true)
            else createSerializerForPrimitiveArray(inputObject, dt)

  given deriveSet[T: Serializer: ClassTag]: Serializer[Set[T]] =
    val forSeq = deriveSeq[List, T]
    new Serializer[Set[T]]:
      override def inputType: DataType = ObjectType(classOf[Set[T]])
      override def serialize(inputObject: Expression) =
        val newInput = Invoke(inputObject, "toSeq", ObjectType(classOf[Seq[_]]))
        forSeq.serialize(newInput)

  inline given deriveMap[K, V](using
      ks: Serializer[K],
      vs: Serializer[V]
  ): Serializer[Map[K, V]] =
    new Serializer[Map[K, V]]:
      override def inputType: DataType = ObjectType(classOf[Map[K, V]])

      override def serialize(inputObject: Expression) =
        createSerializerForMap(
          inputObject,
          MapElementInformation(
            ks.inputType,
            nullable = isNullable(ks.inputType),
            ks.serialize(_)
          ),
          MapElementInformation(
            vs.inputType,
            nullable = isNullable(vs.inputType),
            vs.serialize(_)
          )
        )

  // inspired by https://github.com/apache/spark/blob/39542bb81f8570219770bb6533c077f44f6cbd2a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/ScalaReflection.scala#L575-L599
  inline given derivedProduct[T](using
      mirror: Mirror.ProductOf[T],
      classTag: ClassTag[T]
  ): Serializer[T] =
    new Serializer[T]:
      override def inputType: DataType = ObjectType(classTag.runtimeClass)
      override def serialize(inputObject: Expression): Expression =
        val fields =
          summonAll[mirror.MirroredElemLabels, mirror.MirroredElemTypes].map {
            (label, serializer) =>
              val fieldInputObject =
                Invoke(KnownNotNull(inputObject), label, serializer.inputType)
              (label, serializer.serialize(fieldInputObject))
          }
        createSerializerForObject(inputObject, fields)

  private inline def summonAll[T <: Tuple, U <: Tuple]
      : List[(String, Serializer[?])] =
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
        (constValue[t].toString, summonInline[Serializer[u]]) :: summonAll[
          ts,
          us
        ]

      case _ => Nil
