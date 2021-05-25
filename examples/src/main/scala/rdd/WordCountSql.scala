package rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.*
import org.apache.spark.sql.types.*

import buildinfo.BuildInfo.inputDirectory

@main def wordcountSql =
  val spark = SparkSession.builder().master("local").getOrCreate

  import spark.implicits.{StringToColumn, rddToDatasetHolder}
  import sql.EncoderDerivation.given

  try
    val sc = spark.sparkContext

    val textFile = sc.textFile(inputDirectory.getPath + "/lorem-ipsum.txt")
    val words: Dataset[String] = textFile.flatMap(line => line.split(" ")).toDS


    val counts: Dataset[(String, Double)] = 
      words
        .map(word => (word, 1d))
        .groupByKey((word, _) => word)
        .reduceGroups((a, b) => (a._1, a._2 + b._2))
        .map(_._2)

    counts.show 
  finally
    spark.close()

