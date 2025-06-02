package scala3encoders

import scala3encoders.derivation.{Deserializer, Serializer}
import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.types.DataType

// TODO: Nullable field
given encoder[T](using
    serializer: Serializer[T],
    deserializer: Deserializer[T],
    classTag: ClassTag[T]
): ExpressionEncoder[T] =
  val inputObject = BoundReference(0, serializer.inputType, nullable = true)
  val path = GetColumnByOrdinal(0, deserializer.inputType)

  val agEncoder = new AgnosticEncoder[T] {
    override def dataType: DataType = serializer.inputType
    override def isPrimitive: Boolean = false
    override def clsTag: ClassTag[T] = classTag
  }

  ExpressionEncoder(
    agEncoder,
    serializer.serialize(inputObject),
    deserializer.deserialize(path),
  )
