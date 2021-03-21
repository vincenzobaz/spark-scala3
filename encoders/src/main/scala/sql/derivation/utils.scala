package sql.derivation

private inline def getElemLabels[T <: Tuple]: List[String] = inline compiletime.erasedValue[T] match
  case _: EmptyTuple => Nil
  case _: (t *: ts) => compiletime.constValue[t].toString :: getElemLabels[ts]