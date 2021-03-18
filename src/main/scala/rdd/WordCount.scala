package rdd

import org.apache.spark.sql.SparkSession

@main def wordcount =
  val spark = SparkSession.builder().master("local").getOrCreate
  try
    val sc = spark.sparkContext

    val textFile = sc.textFile("text.txt")
    val counts = textFile.flatMap(line => line.split(" "))
                   .map(word => (word, 1))
                   .reduceByKey(_ + _)
    counts.collect.foreach(println)
  finally
    spark.close()

