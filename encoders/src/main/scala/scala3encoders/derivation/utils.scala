package scala3encoders.derivation

import org.apache.spark.sql.types.DataTypes

private inline def getElemLabels[T <: Tuple]: List[String] = inline compiletime.erasedValue[T] match
  case _: EmptyTuple => Nil
  case _: (t *: ts) => compiletime.constValue[t].toString :: getElemLabels[ts]

val decimalType = DataTypes.createDecimalType(38, 0)