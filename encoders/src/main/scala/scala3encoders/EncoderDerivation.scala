package scala3encoders

import scala3encoders.derivation.{Deserializer, Serializer}
import scala.reflect.ClassTag

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal

// TODO: Nullable field
given encoder[T](using
    serializer: Serializer[T],
    deserializer: Deserializer[T],
    classTag: ClassTag[T]
): ExpressionEncoder[T] =
  val inputObject = BoundReference(0, serializer.inputType, true)
  val path = GetColumnByOrdinal(0, deserializer.inputType)

  ExpressionEncoder(
    serializer.serialize(inputObject),
    deserializer.deserialize(path),
    classTag
  )
