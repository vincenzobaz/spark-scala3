package scala3encoders.derivation

import scala.compiletime.{constValue, summonInline, erasedValue}
import scala.deriving.Mirror
import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  If,
  IsNull,
  Literal
}
import org.apache.spark.sql.catalyst.expressions.objects.NewInstance
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.DeserializerBuildHelper.*
import org.apache.spark.sql.catalyst.expressions.objects._

import org.apache.spark.sql.types.*
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.WalkedTypePath
import org.apache.spark.sql.catalyst.expressions.GetStructField

trait Deserializer[T]:
  def inputType: DataType
  def deserialize(path: Expression): Expression
  def nullable: Boolean = true

object Deserializer:

  // see ScalaReflection.deserializerFor
  inline given deriveOpt[T](using
      d: Deserializer[T],
      ct: ClassTag[T]
  ): Deserializer[Option[T]] =
    new Deserializer[Option[T]]:
      override def inputType: DataType =
        ObjectType(ct.runtimeClass)

      override def deserialize(path: Expression): Expression =
        val tpe = ScalaReflection.typeBoxedJavaMapping.getOrElse(
          d.inputType,
          ct.runtimeClass
        )
        WrapOption(d.deserialize(path), ObjectType(tpe))

  given Deserializer[Int] with
    def inputType: DataType = IntegerType
    def deserialize(path: Expression): Expression =
      createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Integer])
    override def nullable: Boolean = false

  given Deserializer[java.lang.Integer] with
    def inputType: DataType = IntegerType
    def deserialize(path: Expression): Expression =
      createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Integer])
    override def nullable: Boolean = false

  given Deserializer[Long] with
    def inputType: DataType = LongType
    def deserialize(path: Expression): Expression =
      createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Long])
    override def nullable: Boolean = false

  given Deserializer[Double] with
    def inputType: DataType = DoubleType
    def deserialize(path: Expression): Expression =
      createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Double])
    override def nullable: Boolean = false

  given Deserializer[Float] with
    def inputType: DataType = FloatType
    def deserialize(path: Expression): Expression =
      createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Float])
    override def nullable: Boolean = false

  given Deserializer[Short] with
    def inputType: DataType = ShortType
    def deserialize(path: Expression): Expression =
      createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Short])
    override def nullable: Boolean = false

  given Deserializer[Byte] with
    def inputType: DataType = ByteType
    def deserialize(path: Expression): Expression =
      createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Byte])
    override def nullable: Boolean = false

  given Deserializer[Boolean] with
    def inputType: DataType = BooleanType
    def deserialize(path: Expression): Expression =
      createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Boolean])
    override def nullable: Boolean = false

  given Deserializer[java.time.LocalDate] with
    def inputType: DataType = DateType
    def deserialize(path: Expression): Expression =
      createDeserializerForLocalDate(path)

  given Deserializer[java.sql.Date] with
    def inputType: DataType = DateType
    def deserialize(path: Expression): Expression =
      createDeserializerForSqlDate(path)

  given Deserializer[java.time.Instant] with
    def inputType: DataType = TimestampType
    def deserialize(path: Expression): Expression =
      createDeserializerForInstant(path)

  given Deserializer[java.sql.Timestamp] with
    def inputType: DataType = TimestampType
    def deserialize(path: Expression): Expression =
      createDeserializerForSqlTimestamp(path)

  given Deserializer[java.time.Duration] with
    def inputType: DataType = DayTimeIntervalType()
    def deserialize(path: Expression): Expression =
      createDeserializerForDuration(path)

  given Deserializer[java.time.Period] with
    def inputType: DataType = YearMonthIntervalType()
    def deserialize(path: Expression): Expression =
      createDeserializerForPeriod(path)

  /*given deriveEnum[T](using d: Deserializer[T], ct: ClassTag[T]): Deserializer[java.lang.Enum[T]] with
    def inputType: DataType = StringType
    def deserialize(path: Expression): Expression =
      createDeserializerForTypesSupportValueOf(
          Invoke(path, "toString", ObjectType(classOf[String]), returnNullable = false),
          // TODO !!
          ct.getClass())*/

  given Deserializer[String] with
    def inputType: DataType = StringType
    def deserialize(path: Expression): Expression =
      createDeserializerForString(path, false)

  given Deserializer[BigDecimal] with
    def inputType: DataType =
      DecimalType.SYSTEM_DEFAULT
    def deserialize(path: Expression): Expression =
      createDeserializerForJavaBigDecimal(path, returnNullable = false)

  given Deserializer[java.math.BigInteger] with
    def inputType: DataType =
      DecimalType(38, 0) // .BigIntDecimal is private
    def deserialize(path: Expression): Expression =
      createDeserializerForJavaBigInteger(path, returnNullable = false)

  given Deserializer[scala.math.BigInt] with
    def inputType: DataType =
      DecimalType(38, 0) // .BigIntDecimal is private
    def deserialize(path: Expression): Expression =
      createDeserializerForScalaBigInt(path)

  inline given deriveArray[T](using
      d: Deserializer[T],
      ct: ClassTag[T]
  ): Deserializer[Array[T]] =
    // TODO: nullable. walked
    new Deserializer[Array[T]]:
      override def inputType: DataType = ArrayType(d.inputType)
      override def deserialize(path: Expression): Expression =
        val mapFunction: Expression => Expression = el =>
          deserializerForWithNullSafetyAndUpcast(
            el,
            d.inputType,
            true,
            WalkedTypePath(Nil),
            (casted, _) => d.deserialize(casted)
          )
        val arrayClass = ObjectType(ct.newArray(0).getClass)
        val arrayData = UnresolvedMapObjects(mapFunction, path)

        val methodName = d.inputType match
          case IntegerType => "toIntArray"
          case LongType    => "toLongArray"
          case DoubleType  => "toDoubleArray"
          case FloatType   => "toFloatArray"
          case ShortType   => "toShortArray"
          case ByteType    => "toByteArray"
          case BooleanType => "toBooleanArray"
          // non-primitive
          case _ => "array"

        Invoke(arrayData, methodName, arrayClass, returnNullable = true)

  inline given deriveSeq[F[_], T](using d: Deserializer[T], ct: ClassTag[T])(
      using F[T] <:< Seq[T]
  ): Deserializer[F[T]] =
    // TODO: Nullable
    new Deserializer[F[T]]:
      override def inputType: DataType = ArrayType(d.inputType)
      override def deserialize(path: Expression): Expression =
        val mapFunction: Expression => Expression = element =>
          deserializerForWithNullSafetyAndUpcast(
            element,
            d.inputType,
            nullable = true,
            WalkedTypePath(Nil),
            (casted, _) => d.deserialize(casted)
          )
        UnresolvedMapObjects(mapFunction, path, Some(classOf[Seq[T]]))

  inline given derivedSet[T: Deserializer: ClassTag]: Deserializer[Set[T]] =
    val forSeq = deriveSeq[List, T]
    new Deserializer[Set[T]]:
      override def inputType: DataType = forSeq.inputType
      override def deserialize(path: Expression): Expression =
        val res = forSeq.deserialize(path).asInstanceOf[UnresolvedMapObjects]
        UnresolvedMapObjects(res.function, res.child, Some(classOf[Set[T]]))

  inline given derivedMap[K, V](using
      kd: Deserializer[K],
      vd: Deserializer[V],
      ct: ClassTag[Map[K, V]]
  ): Deserializer[Map[K, V]] =
    new Deserializer[Map[K, V]]:
      override def inputType: DataType = MapType(kd.inputType, vd.inputType)
      override def deserialize(path: Expression): Expression =
        UnresolvedCatalystToExternalMap(
          path,
          kd.deserialize(_),
          vd.deserialize(_),
          ct.runtimeClass
        )

// inspired by https://github.com/apache/spark/blob/39542bb81f8570219770bb6533c077f44f6cbd2a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/ScalaReflection.scala#L356-L390
  inline given derivedProduct[T](using
      mirror: Mirror.ProductOf[T],
      classTag: ClassTag[T]
  ): Deserializer[T] =
    val elems = summonAll[mirror.MirroredElemLabels, mirror.MirroredElemTypes]
    lazy val fields = elems
      .map((label, deserializer) =>
        StructField(label, deserializer.inputType, deserializer.nullable)
      )
    def cls = classTag.runtimeClass
    def isTuple = cls.getName.startsWith("scala.Tuple")

    new Deserializer[T]:
      override def inputType: DataType = StructType(fields)
      override def deserialize(path: Expression): Expression =
        val arguments = elems.zipWithIndex.map {
          case ((label, deserializer), i) =>
            val newPath =
              if (isTuple) then GetStructField(path, i)
              else UnresolvedExtractValue(path, Literal(label))
            deserializer.deserialize(newPath)
        }
        NewInstance(cls, arguments, ObjectType(cls), false)

  private inline def summonAll[T <: Tuple, U <: Tuple]
      : List[(String, Deserializer[?])] =
    inline (erasedValue[T], erasedValue[U]) match
      // same bulk processing as in Serializer to prevent stackoverflow on summoning decoders for large case classes
      case _: (
              t1 *: t2 *: t3 *: t4 *: t5 *: t6 *: t7 *: t8 *: t9 *: t10 *:
                t11 *: t12 *: t13 *: t14 *: t15 *: t16 *: ts,
              u1 *: u2 *: u3 *: u4 *: u5 *: u6 *: u7 *: u8 *: u9 *: u10 *:
                u11 *: u12 *: u13 *: u14 *: u15 *: u16 *: us
          ) =>
        List(
          (constValue[t1].toString, summonInline[Deserializer[u1]]),
          (constValue[t2].toString, summonInline[Deserializer[u2]]),
          (constValue[t3].toString, summonInline[Deserializer[u3]]),
          (constValue[t4].toString, summonInline[Deserializer[u4]]),
          (constValue[t5].toString, summonInline[Deserializer[u5]]),
          (constValue[t6].toString, summonInline[Deserializer[u6]]),
          (constValue[t7].toString, summonInline[Deserializer[u7]]),
          (constValue[t8].toString, summonInline[Deserializer[u8]]),
          (constValue[t9].toString, summonInline[Deserializer[u9]]),
          (constValue[t10].toString, summonInline[Deserializer[u10]]),
          (constValue[t11].toString, summonInline[Deserializer[u11]]),
          (constValue[t12].toString, summonInline[Deserializer[u12]]),
          (constValue[t13].toString, summonInline[Deserializer[u13]]),
          (constValue[t14].toString, summonInline[Deserializer[u14]]),
          (constValue[t15].toString, summonInline[Deserializer[u15]]),
          (constValue[t16].toString, summonInline[Deserializer[u16]])
        )
          ::: summonAll[ts, us]
      case _: ((t *: ts), (u *: us)) =>
        (constValue[t].toString, summonInline[Deserializer[u]]) :: summonAll[
          ts,
          us
        ]
      case _ => Nil
