package scala3encoders

import org.apache.spark.sql.SparkSession

trait SparkSqlTesting extends munit.Suite:
  export spark.implicits._
  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate

  override def afterAll(): Unit =
    spark.stop()
