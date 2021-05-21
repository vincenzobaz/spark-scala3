package sql.derivation

import scala.compiletime
import scala.deriving.Mirror
import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.expressions.{Expression, If, IsNull, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.NewInstance
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.DeserializerBuildHelper.*
import org.apache.spark.sql.catalyst.expressions.objects._

import org.apache.spark.sql.types.*
import org.apache.spark.sql.catalyst.WalkedTypePath

trait Deserializer[T]:
  def inputType: DataType
  def deserialize(path: Expression): Expression

object Deserializer:
  given Deserializer[String] with
    def inputType: DataType = StringType
    def deserialize(path: Expression): Expression = 
      createDeserializerForString(path, false)

  given Deserializer[Int] with
    def inputType: DataType = IntegerType
    def deserialize(path: Expression): Expression =
      createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Integer])

  given Deserializer[Long] with
    def inputType: DataType = LongType
    def deserialize(path: Expression): Expression =
      createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Long])

  inline given deriveOpt[T](using d: Deserializer[T]): Deserializer[Option[T]] =
    new Deserializer[Option[T]]:
      override def inputType: DataType = d.inputType
      override def deserialize(path: Expression): Expression =
        WrapOption(d.deserialize(path), d.inputType)
 
  inline given derived[T](using m: Mirror.Of[T], ct: ClassTag[T]): Deserializer[T] = 
    inline m match
      case p: Mirror.ProductOf[T] => product(p, ct)
      case s: Mirror.SumOf[T] => compiletime.error("Cannot derive Deserializer for Sum types")

  // inspired by https://github.com/apache/spark/blob/39542bb81f8570219770bb6533c077f44f6cbd2a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/ScalaReflection.scala#L356-L390
  private inline def product[T](mirror: Mirror.ProductOf[T], classTag: ClassTag[T]): Deserializer[T] = 
    val deserializers: List[Deserializer[?]] = summonTuple[mirror.MirroredElemTypes]
    val labels: List[String] = getElemLabels[mirror.MirroredElemLabels]
    val fields = labels.zip(deserializers)
      .map((label, deserializer) => StructField(label, deserializer.inputType))
    new Deserializer[T]:
      override def inputType: StructType = StructType(fields)
      override def deserialize(path: Expression): Expression =
        val arguments = inputType.fields.toSeq
          .zip(deserializers)
          .map { (structField, deserializer) =>
            val newPath = UnresolvedExtractValue(path, Literal(structField.name))
            deserializer.deserialize(newPath)
          }
        val outputType = ObjectType(classTag.runtimeClass)
        If(
          IsNull(path),
          Literal.create(null, outputType),
          NewInstance(outputType.cls, arguments, outputType, false)
        )

  private inline def summonTuple[T <: Tuple]: List[Deserializer[?]] = inline compiletime.erasedValue[T] match
    case _: EmptyTuple => Nil
    case _: (t *: ts) => compiletime.summonInline[Deserializer[t]] :: summonTuple[ts]

