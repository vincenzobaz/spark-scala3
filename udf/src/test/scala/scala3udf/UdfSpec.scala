package scala3udftest

import org.apache.spark.sql.functions.*

import scala3udf.{
  // "old" udf doesn't interfer with new scala3udf.udf when renamed
  Udf => udf
}
import scala3encoders.given

import org.apache.spark.sql.SparkSession

case class DataWithPos(name: String, x: Int, y: Int, z: Int)
case class DataWithX(name: String, x: Int)
case class DataWithOptX(name: Option[String], x: Int)
case class ContainsData(datax: DataWithX)
case class ContainsDataOpt(dataoptx: DataWithOptX)
case class MirrorPos(mirror: (Int, Int, Int))
case class PosOuter(pos: (Int, Int, Int))

class UdfSpec extends munit.FunSuite:
  given spark: SparkSession = SparkSession.builder().master("local").getOrCreate

  import spark.sqlContext.implicits._

  override def afterAll(): Unit =
    spark.stop()

  test("select udf returning a tuple") {
    udf((x: Int, y: Int, z: Int) => (-x, -y, z)).register("mirror")

    val input =
      Seq(DataWithPos("zero", 0, 0, 0), DataWithPos("something", 1, 2, 3))
    val df = input.toDF()
    df.createOrReplaceTempView("data")

    // the resulting column is named mirror and contains a tuple with 3 elements
    val res = spark
      .sql("SELECT mirror(x,y,z) as mirror FROM data")
      .as[MirrorPos]
      .collect()
      .toList

    val cmp = input.map { case DataWithPos(name, x, y, z) =>
      MirrorPos(-x, -y, z)
    }
    assertEquals(cmp, res)
  }

  test("select udf returning a case class") {
    udf((name: String, x: Int) => DataWithX(name, 2 * x)).register("datax")
    val input =
      Seq(DataWithPos("zero", 0, 0, 0), DataWithPos("something", 1, 2, 3))
    val df = input.toDF()
    df.createOrReplaceTempView("data")

    // the resulting column is named datax and contains a tuple with 2 elements
    val res = spark
      .sql("SELECT datax(name, x) as datax FROM data")
      .as[ContainsData]
      .collect()
      .toList

    val cmp = input.map { case DataWithPos(name, x, y, z) =>
      ContainsData(DataWithX(name, 2 * x))
    }
    assertEquals(cmp, res)
  }

  test("select udf returning an option in case class") {
    udf((name: String, x: Int) => DataWithOptX(Option(name), 2 * x))
      .register("dataoptx")
    val input =
      Seq(DataWithPos(null, 0, 0, 0), DataWithPos("something", 1, 2, 3))
    val df = input.toDF()
    df.createOrReplaceTempView("data")
    val res = spark
      .sql("SELECT dataoptx(name, x) as dataoptx from data")
      .as[ContainsDataOpt]
      .collect()
      .toList

    val cmp = input.map { case DataWithPos(name, x, y, z) =>
      ContainsDataOpt(DataWithOptX(Option(name), 2 * x))
    }
    assertEquals(res(0).dataoptx.name, None)
    assertEquals(res(1).dataoptx.name, Some("something"))
    assertEquals(cmp, res)
  }

  test("select random") {
    udf(() => Math.random()).register("random")
    val df = Seq(0, 0, 0, 0, 0).toDF()
    df.createOrReplaceTempView("data")
    val res = spark.sql("SELECT random() from data").as[Double].collect().toList
    assert(res.size == res.distinct.size)
  }

  test("local udf without register") {
    val df = Seq(1, 2, 3, 4, 5).toDF("x")
    df.createOrReplaceTempView("data")
    val fun = udf((i: Int) => 2 * i + 1)
    val res = df.select(fun(factorial($"x"))).as[Int].collect().toList
    assert(res == List(3, 5, 13, 49, 241))
  }

  test("using a local class") {
    val input = List(PosOuter(0, 0, 0), PosOuter(-1, -1, 2))
    val df =
      input.map { case PosOuter((x, y, z)) => DataWithPos("", x, y, z) }.toDF()

    // a local class could be used inside a udf
    case class Pos(pos: (Int, Int, Int))
    val pos = udf((x: Int, y: Int, z: Int) => Pos((x, y, z)))

    val dfOut = df.select(pos($"x", $"y", $"z").as("p")).select("p.*")

    // but we need a globally accessible class for decoding
    val res = dfOut.as[PosOuter].collect().toList
    assert(res == input)

    // error message changed slightly with spark versions
    val dot = if (spark.version.split("\\.")(1).toInt <= 3) "" else "."
    // and this one throws since Pos could not be decoded
    interceptMessage[RuntimeException](
      """Error while decoding: scala.ScalaReflectionException: <none> is not a term
        |newInstance(class scala3udftest.UdfSpec$Pos$1)""".stripMargin + dot
    ) {
      // spark compiler spills out some error messages - ignore them here
      spark.sparkContext.setLogLevel("FATAL")
      dfOut.as[Pos].collect()
      spark.sparkContext.setLogLevel("INFO")
    }
  }
