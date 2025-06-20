package scala3encoders

import org.apache.spark.sql.{AnalysisException, Encoder}
import org.apache.spark.sql.types.*
import org.apache.spark.sql.functions.*
import java.io.{File, PrintWriter}
import java.time.{Instant, LocalDate}
import scala.concurrent.duration.*
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder

case class A()
case class B(x: String)
case class C(x: Int, y: Long)
case class D(x: String, y: B)
case class E(x: Map[String, Seq[String]], y: Array[Int], z: Set[Double])
case class F(v: Option[String], w: Option[Long], p: (Int, Int, Int))
case class G(x: BigDecimal, y: BigInt)
case class H(x: java.math.BigDecimal, y: java.math.BigInteger)
case class ChkCustom(u: Option[Instant])
case class ChkTuple(
    tup: (String, Option[Int]),
    content: Map[(String, LocalDate), Long],
    i: Int
)
case class Pos(x: Int, y: Int, z: Long)

final case class Key(id: String, date: LocalDate)
final case class ChkMap(
    id: Int,
    m: Map[Key, Long]
)

case class Sequence(id: Int, nums: Seq[Int])

case class City(name: String, lat: Double, lon: Double)
case class CityWithInts(name: String, lat: Int, lon: Int)
case class Journey(id: Int, cities: Seq[City])
case class DurationData(duration: FiniteDuration)

enum Color:
  case Red, Black
case class ColorData(color: Color)

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
    assertEquals(input.toDS().collect.toSeq, input)
  }

  test("derive encoder of case class B(x: String)") {
    val encoder = summon[Encoder[B]]
    assertEquals(
      encoder.schema,
      StructType(Seq(StructField("x", StringType, nullable = true)))
    )

    val input = Seq(B("hello"), B("world"))
    assertEquals(input.toDS().collect.toSeq, input)
  }

  test("derive encoder of case class C(x: Int, y: Long)") {
    val encoder = summon[Encoder[C]].asInstanceOf[AgnosticEncoder[C]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("x", IntegerType, nullable = false),
          StructField("y", LongType, nullable = false)
        )
      )
    )

    val input =
      Seq(C(42, -9_223_372_036_854_775_808L), C(0, 9_223_372_036_854_775_807L))
    assertEquals(input.toDS().collect.toSeq, input)
  }

  test("derive encoder of case class Pos and collect as tuple") {
    val encoder = summon[Encoder[Pos]].asInstanceOf[AgnosticEncoder[Pos]]
    val input = Seq(Pos(1, 2, 3L), Pos(4, 5, 6))

    val res = input
      .toDF()
      .select(col("x"), col("z"))
      .distinct
      .as[(Int, Long)]
      .collect()
      .toSet
    val cmp = Set[(Int, Long)]((1, 3L), (4, 6L))
    assertEquals(res, cmp)
  }

  test(
    "derive encoder of case class D(x: String, y: B) where B is case class B(x: String)"
  ) {
    val encoder = summon[Encoder[D]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("x", StringType, nullable = true),
          StructField(
            "y",
            StructType(Seq(StructField("x", StringType, nullable = true))),
            nullable = true
          )
        )
      )
    )

    val input = Seq(D("Hello", B("World")), D("Bye", B("Universe")))
    assertEquals(input.toDS().collect.toSeq, input)
  }

  test(
    "derive encoder of case class E(x: Map[String, Seq[String]], y: Array[Int], z: Set[Double])"
  ) {
    val encoder = summon[Encoder[E]]
    val input = Seq(
      E(Map("a" -> Seq("foo", "bar")), Array(1, 2, 3, 4, 5), Set(1.0, 2.0)),
      E(Map.empty, Array(1, 2), Set(1.0, 2.0)),
      E(Map(), Array.empty, Set.empty)
    )
    val res = input.toDS().collect.toSeq
    assertEquals(res.map(_.x), input.map(_.x))
    assert(
      res
        .map(_.y)
        .zip(input.map(_.y))
        .forall(y => (y._1 == null && y._2 == null) || y._1.sameElements(y._2))
    )
    assertEquals(res.map(_.z), input.map(_.z))
  }

  test(
    "derive encoder of case class F"
  ) {
    val encoder = summon[Encoder[F]]
    val input = Seq(
      F(Some("whoo"), None, (1, 2, 3)),
      F(None, Some(1L), (-1, 0, 1))
    )
    val res = input.toDS().collect.toSeq
    assertEquals(res(0), input(0))
    assertEquals(res(1), input(1))
  }

  test(
    "derive encoder of case class G"
  ) {
    val encoder = summon[Encoder[G]]
    val input = Seq(
      G(BigDecimal(1.0001), BigInt(1)),
      G(BigDecimal(0.0), BigInt(0)),
      G(BigDecimal(-1.0001), BigInt(-1))
    )
    val res = input.toDS()
    assertEquals(res.dtypes(0), ("x", "DecimalType(38,18)"))
    assertEquals(res.dtypes(1), ("y", "DecimalType(38,0)"))
    assertEquals(res.collect.toSeq, input)
  }

  test(
    "derive encoder of case class H"
  ) {
    val encoder = summon[Encoder[H]]
    val input = Seq(
      H(
        new java.math.BigDecimal("1.000100000000000000"),
        new java.math.BigInteger("1")
      ),
      H(new java.math.BigDecimal("0E-18"), new java.math.BigInteger("0")),
      H(
        new java.math.BigDecimal("-1.000100000000000000"),
        new java.math.BigInteger("-1")
      )
    )
    val res = input.toDS()
    assertEquals(res.dtypes(0), ("x", "DecimalType(38,18)"))
    assertEquals(res.dtypes(1), ("y", "DecimalType(38,0)"))

    assertEquals(res.collect.toSeq, input)
  }

  test(
    "derive encoder of case class ChkTuple"
  ) {
    val encoder = summon[Encoder[ChkTuple]]
    val input = Seq(
      ChkTuple(("Foo", Some(1)), Map(), 1),
      ChkTuple(
        ("Bar", None),
        Map(
          ("foo", LocalDate.now().minusDays(10)) -> 1L,
          ("bar", LocalDate.now().minusDays(1)) -> 2L
        ),
        2
      )
    )
    assertEquals(input.toDS().collect.toSeq, input)
  }

  test("derive encoder of case class A()") {
    val encoder = summon[Encoder[A]]
    assertEquals(encoder.schema, StructType(Seq.empty))

    val input = Seq(A(), A())
    assertEquals(input.toDS().collect.toSeq, input)
  }

  /* TODO
  test("derive encoder of FiniteDuration") {
    val data = Seq(ColorData(Color.Black), ColorData(Color.Red))
      .toDS()
      .map(_.copy(Color.Red))
    assertEquals(
      data.schema,
      StructType(Seq(StructField("color", StringType, true)))
    )
    assertEquals(
      data.collect().toSeq,
      Seq(ColorData(Color.Red), ColorData(Color.Red))
    )
  }*/

  test("List[Int]") {
    val ls = List(List(1, 2, 3), List(4, 5, 6))
    assertEquals(ls.toDS().collect().toList, ls)
  }

  test("List[case class]") {
    val seq1 = Sequence(0, 2 :: 3 :: Nil)
    val seq2 = Sequence(1, 4 :: 5 :: Nil)
    val seq3 = Sequence(2, 7 :: 8 :: 9 :: Nil)

    val seqs = seq1 :: seq2 :: seq3 :: Nil

    assertEquals(
      seqs.toDS().collect().toList,
      seqs
    )
  }

  test("Issue 14") {
    val rome = City("Rome", 41.9, 12.49)
    val paris = City("Paris", 48.8, 2.34)
    val berlin = City("Berlin", 52.52, 13.40)
    val madrid = City("Madrid", 40.41, -3.70)

    val trip1 = Journey(0, rome :: paris :: Nil)
    val trip2 = Journey(1, berlin :: madrid :: Nil)
    val trip3 = Journey(2, berlin :: madrid :: paris :: Nil)

    val trips = trip1 :: trip2 :: trip3 :: Nil

    val idsIncrement = trips.toDS().map(tr => tr.copy(id = tr.id + 1))

    assertEquals(
      idsIncrement.collect().toList,
      trips.map(tr => tr.copy(id = tr.id + 1))
    )
  }

  test("ChkMap") {
    val encoder = summon[Encoder[ChkMap]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("id", IntegerType, false),
          StructField(
            "m",
            MapType(
              StructType(
                Seq(
                  StructField("id", StringType, nullable = true),
                  StructField("date", DateType, nullable = true)
                )
              ),
              LongType,
              valueContainsNull = false
            ),
            nullable = true
          )
        )
      )
    )

    val input = Seq(
      ChkMap(
        0,
        Map(Key("foo", LocalDate.now().minusDays(10)) -> 123L)
      )
    )
    assertEquals(input.toDS().collect.toSeq, input)
  }

  test("derive encoder of FiniteDuration") {
    val data = Seq(DurationData(1.minute), DurationData(2.seconds))
      .toDS()
      .map(row => row.copy(duration = row.duration * 2))
    assertEquals(
      data.schema,
      StructType(
        Seq(
          StructField(
            "duration",
            DayTimeIntervalType(startField = 0, endField = 3),
            false
          )
        )
      )
    )
    assertEquals(
      data.collect().toSeq,
      Seq(DurationData(2.minute), DurationData(4.seconds))
    )
  }

  if (spark.version.split("\\.")(1).toInt > 3) then
    // cast check is only supported for spark version > 3.3.x
    test("check cast is checked") {
      // create temporary csv file
      val file = File.createTempFile("test", ".csv")
      val lines = """name;lat;lon
                  |Berlin;52.520008;13.40
                  |Madrid;40.416775;-3.70
                  |New York;40.730610;-73.935242
                  |""".stripMargin
      // write lines to file
      val pw = new PrintWriter(file)
      pw.write(lines)
      pw.close()

      val df = spark.sqlContext.read
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "true")
        .csv(file.getAbsolutePath)

      val exception = intercept[AnalysisException] {
        df.as[CityWithInts]
      }
      assert(
        clue(exception.getMessage).contains(
          "[CANNOT_UP_CAST_DATATYPE] Cannot up cast lat from \"DOUBLE\" to \"INT\"."
        )
      )

      val cities = df.as[City]
      assertEquals(
        cities.collect.toList,
        List(
          City("Berlin", 52.520008, 13.40),
          City("Madrid", 40.416775, -3.70),
          City("New York", 40.730610, -73.935242)
        )
      )
    }
