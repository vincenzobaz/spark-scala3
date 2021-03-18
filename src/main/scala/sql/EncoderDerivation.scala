package sql

import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.compiletime.{constValue, erasedValue, summonInline, error}

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

object EncoderDerivation:
  given Encoder[Array[Byte]] = Encoders.BINARY
  given Encoder[String] = Encoders.STRING
  given Encoder[Boolean] = Encoders.scalaBoolean
  given Encoder[Byte] = Encoders.scalaByte
  given Encoder[Short] = Encoders.scalaShort
  given Encoder[Double] = Encoders.scalaDouble
  given Encoder[Int] = Encoders.scalaInt
  given Encoder[Long] = Encoders.scalaLong
  given Encoder[Float] = Encoders.scalaFloat
  given Encoder[java.sql.Date] = Encoders.DATE
  given Encoder[java.sql.Timestamp] = Encoders.TIMESTAMP
  given Encoder[java.time.Instant] = Encoders.INSTANT
  given Encoder[java.time.LocalDate] = Encoders.LOCALDATE
  given Encoder[java.math.BigDecimal] = Encoders.DECIMAL

  // TODO: Nullable field
  inline given derived[T](using m: Mirror.Of[T], ct: ClassTag[T]): Encoder[T] = inline m match
    case p: Mirror.ProductOf[T] => productEncoder(p, ct)
    case s: Mirror.SumOf[T] => error("Encoders cannot be derived for Sum types. An implicit Encoder[T] is needed to store T instances in a Dataset. Primitive types (Int, String, etc) and Product types (case classes) are supported")

  private inline def getNames[T <: Tuple]: List[String] = inline erasedValue[T] match
    case _: EmptyTuple => Nil
    case _: (t *: ts) => constValue[t].toString :: getNames[ts]

  private inline def summonEncodersFromTuple[T <: Tuple]: List[Encoder[_]] = inline erasedValue[T] match
    case _: EmptyTuple => Nil
    case _: (t *: ts) => summonInline[Encoder[t]] :: summonEncodersFromTuple[ts]
    
  private inline def productEncoder[T](mirror: Mirror.ProductOf[T], ct: ClassTag[T]): Encoder[T] =
    val encoders: List[Encoder[_]] = summonEncodersFromTuple[mirror.MirroredElemTypes]
    val fieldNames: List[String] = getNames[mirror.MirroredElemLabels]
 
    new Encoder[T] {
      override def clsTag: ClassTag[T] = ct
      override def schema: StructType =
        val fields = fieldNames.zip(encoders).map { (label, encoder) => 
          val dt: DataType =
            if encoder.schema.length > 1 then StructType(encoder.schema.fields)
            else encoder.schema.fields.head.dataType
          StructField(label, dt)
        }
        StructType(fields)
    }