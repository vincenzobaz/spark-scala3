package scala3udftest

import org.apache.spark.sql.functions.*

import scala3udf.{
  // "old" udf doesn't interfer with new scala3udf.udf when renamed
  Udf => udf
}
import scala3encoders.given

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder

case class DataWithPos(name: String, x: Int, y: Int, z: Int)
case class DataWithX(name: String, x: Int)
case class DataWithOptX(name: Option[String], x: Int)
case class ContainsData(datax: DataWithX)
case class ContainsDataOpt(dataoptx: DataWithOptX)
case class MirrorPos(mirror: (Int, Int, Int))

val mirror = udf((x: Int, y: Int, z: Int) => (-x, -y, z))
val datax = udf((name: String, x: Int) => DataWithX(name, 2 * x))
val dataoptx = udf((name: String, x: Int) => DataWithOptX(Option(name), 2 * x))
val random = udf(() => Math.random())

class UdfSpec extends munit.FunSuite:
  given spark: SparkSession = SparkSession.builder().master("local").getOrCreate
  udf.register(mirror, datax, dataoptx, random)

  import spark.sqlContext.implicits._

  override def afterAll(): Unit =
    spark.stop()

  test("select udf returning a tuple") {
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

  test("local udf with register throws") {
    interceptMessage[IllegalArgumentException](
      "provided function has to be moved to package level!"
    ) {
      udf((i: Int) => 0).register("local")
    }
  }
