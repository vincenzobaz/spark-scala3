package sql

case class Test0(f1: String)
case class Test1(f1: String, f2: Int)
case class Test2(token: String, tx: Long)
case class Test3(x: Int, t2: Test2)

enum Sum:
  case A(x: Int)
  case B(y: Boolean)
  case C(z: String)

case class SumWrapper(aOrB: Sum)
