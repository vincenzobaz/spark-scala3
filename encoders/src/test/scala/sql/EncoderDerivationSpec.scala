package scala3encoders

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

case class A()
case class B(x: String)
case class C(x: Int, y: Long)
case class D(x: String, y: B)

val dSchema =
  StructType(
    Seq(
      StructField("x", StringType),
      StructField("y", StructType(Seq(StructField("x", StringType))))
    )
  )

class EncoderDerivationSpec extends munit.FunSuite with SparkSqlTesting:
  test("derive encoder of case class A()") {
    val encoder = summon[Encoder[A]]
    assertEquals(encoder.schema, StructType(Seq.empty))

    val input = Seq(A(), A())
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test("derive encoder of case class B(x: String)") {
    val encoder = summon[Encoder[B]]
    assertEquals(
      encoder.schema,
      StructType(Seq(StructField("x", StringType)))
    )

    val input = Seq(B("hello"), B("world"))
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test("derive encoder of case class C(x: Int, y: Long)") {
    val encoder = summon[Encoder[C]].asInstanceOf[ExpressionEncoder[C]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("x", IntegerType),
          StructField("y", LongType)
        )
      )
    )

    val input =
      Seq(C(42, -9_223_372_036_854_775_808L), C(0, 9_223_372_036_854_775_807L))
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test(
    "derive encoder of case class D(x: String, y: B) where B is case class B(x: String)"
  ) {
    val encoder = summon[Encoder[D]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("x", StringType),
          StructField("y", StructType(Seq(StructField("x", StringType))))
        )
      )
    )

    val input = Seq(D("Hello", B("World")), D("Bye", B("Universe")))
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test("derive encoder of case class A()") {
    val encoder = summon[Encoder[A]]
    assertEquals(encoder.schema, StructType(Seq.empty))

    val input = Seq(A(), A())
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test("List[Int]") {
    val ls = List(List(1,2,3), List(4,5,6))
    assertEquals(ls.toDS.collect().toList, ls)
  }

  test("List[case class]") {
    case class Sequence(id: Int, nums: List[Int])

    val seq1 = Sequence(0, 2 :: 3 :: Nil)
    val seq2 = Sequence(1, 4 :: 5 :: Nil)
    val seq3 = Sequence(2, 7 :: 8 :: 9 :: Nil)

    val seqs = seq1 :: seq2 :: seq3 :: Nil

    assertEquals(
      seqs.toDS.collect().toList,
      seqs
    )
  }

  test("Issue 14") {
    case class City(name: String, lat: Double, lon: Double)
    case class Journey(id: Int, cities: List[City])

    val rome = City("Rome", 41.9, 12.49)
    val paris = City("Paris", 48.8, 2.34)
    val berlin = City("Berlin", 52.52, 13.40)
    val madrid = City("Madrid", 40.41, -3.70)

    val trip1 = Journey(0, rome :: paris :: Nil)
    val trip2 = Journey(1, berlin :: madrid :: Nil)
    val trip3 = Journey(2, berlin :: madrid :: paris :: Nil)

    val trips = trip1 :: trip2 :: trip3 :: Nil

    val idsIncrement = trips.toDS.map(tr => tr.copy(id = tr.id + 1))

    assertEquals(
      idsIncrement.collect().toList,
      trips.map(tr => tr.copy(id = tr.id + 1))
    )
  }
