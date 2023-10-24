package scala3encoders

import org.apache.spark.sql.{AnalysisException, Encoder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import java.io.{File, PrintWriter}

case class A()
case class B(x: String)
case class C(x: Int, y: Long)
case class D(x: String, y: B)
case class E(x: Map[String, Seq[String]], y: Array[Int], z: Set[Double])
case class F(v: Option[String], w: Option[Long], p: (Int, Int, Int))
case class Dates(u: java.time.Instant)
case class ChkTuple(
    tup: (String, Option[Int]),
    content: Map[(String, java.time.LocalDate), Long],
    i: Int
)
case class Pos(x: Int, y: Int, z: Long)
case class Big(
    i00: Int,
    i01: Int,
    i02: Int,
    i03: Int,
    i04: Int,
    i05: Int,
    i06: Int,
    i07: Int,
    i08: Int,
    i09: Int,
    i10: Int,
    i11: Int,
    i12: Int,
    i13: Int,
    i14: Int,
    i15: Int,
    i16: Int,
    i17: Int,
    i18: Int,
    i19: Int,
    i20: Int,
    i21: Int,
    i22: Int,
    i23: Int,
    i24: Int,
    i25: Int,
    i26: Int,
    i27: Int,
    i28: Int,
    i29: Int,
    i30: Int,
    i31: Int,
    i32: Int,
    i33: Int,
    i34: Int,
    i35: Int,
    i36: Int,
    i37: Int,
    i38: Int,
    i39: Int,
    i40: Int,
    i41: Int,
    i42: Int,
    i43: Int,
    i44: Int,
    i45: Int,
    i46: Int,
    i47: Int,
    i48: Int,
    i49: Int,
    i50: Int,
    i51: Int,
    i52: Int,
    i53: Int,
    i54: Int,
    i55: Int,
    i56: Int,
    i57: Int,
    i58: Int,
    i59: Int,
    i60: Int,
    i61: Int,
    i62: Int,
    i63: Int,
    i64: Int,
    i65: Int,
    i66: Int,
    i67: Int,
    i68: Int,
    i69: Int,
    i70: Int,
    i71: Int,
    i72: Int,
    i73: Int,
    i74: Int,
    i75: Int,
    i76: Int,
    i77: Int,
    i78: Int,
    i79: Int,
    i80: Int,
    i81: Int,
    i82: Int,
    i83: Int,
    i84: Int,
    i85: Int,
    i86: Int,
    i87: Int,
    i88: Int,
    i89: Int,
    i90: Int,
    i91: Int,
    i92: Int,
    i93: Int,
    i94: Int,
    i95: Int,
    i96: Int,
    i97: Int,
    i98: Int,
    i99: Int
)

final case class Key(id: String, date: java.time.LocalDate)
final case class ChkMap(
    id: Int,
    m: Map[Key, Long]
)

case class Sequence(id: Int, nums: Seq[Int])

case class City(name: String, lat: Double, lon: Double)
case class CityWithInts(name: String, lat: Int, lon: Int)
case class Journey(id: Int, cities: Seq[City])

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

  test("derive encoder of case class Pos and collect as tuple") {
    val encoder = summon[Encoder[Pos]].asInstanceOf[ExpressionEncoder[Pos]]
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
          StructField("x", StringType),
          StructField("y", StructType(Seq(StructField("x", StringType))))
        )
      )
    )

    val input = Seq(D("Hello", B("World")), D("Bye", B("Universe")))
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test(
    "derive encoder of case class E(x: Map[String, Seq[String]], y: Array[Int], z: Set[Double], u: java.time.Instant)"
  ) {
    val encoder = summon[Encoder[E]]
    val input = Seq(
      E(Map("a" -> Seq("foo", "bar")), Array(1, 2, 3, 4, 5), Set(1.0, 2.0)),
      E(null, Array(1, 2), Set(1.0, 2.0)),
      E(Map(), null, null)
    )
    val res = input.toDS.collect.toSeq
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
      F(None, Some(1L), (-1, 0, 1)),
      F(null, null, (0, 0, 0))
    )
    val res = input.toDS.collect.toSeq
    assertEquals(res(0), input(0))
    assertEquals(res(1), input(1))
    // null will be mapped to None
    assertEquals(res(2)._1, None)
    assertEquals(res(2)._2, None)
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
          ("foo", java.time.LocalDate.now().minusDays(10)) -> 1L,
          ("bar", java.time.LocalDate.now().minusDays(1)) -> 2L
        ),
        2
      )
    )
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test("derive encoder of case class A()") {
    val encoder = summon[Encoder[A]]
    assertEquals(encoder.schema, StructType(Seq.empty))

    val input = Seq(A(), A())
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test("List[Int]") {
    val ls = List(List(1, 2, 3), List(4, 5, 6))
    assertEquals(ls.toDS.collect().toList, ls)
  }

  test("List[case class]") {
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

  test("ChkMap") {
    val encoder = summon[Encoder[ChkMap]]
    assertEquals(
      encoder.schema,
      StructType(
        Seq(
          StructField("id", IntegerType, true),
          StructField(
            "m",
            MapType(
              StructType(
                Seq(
                  StructField("id", StringType, true),
                  StructField("date", DateType, true)
                )
              ),
              LongType,
              false
            ),
            true
          )
        )
      )
    )

    val input = Seq(
      ChkMap(
        0,
        Map(Key("foo", java.time.LocalDate.now().minusDays(10)) -> 123L)
      )
    )
    assertEquals(input.toDS.collect.toSeq, input)
  }

  test("check Big class") {
    val encoder = summon[Encoder[Big]]
    val input = Seq(
      Big(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37,
        38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,
        56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73,
        74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91,
        92, 93, 94, 95, 96, 97, 98, 99)
    )
    assertEquals(input.toDS.collect.toSeq, input)
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
        .csv(file.getAbsolutePath())

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
