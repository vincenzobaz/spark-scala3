//package sql
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.Encoder

//@main def encoderBroken =
//  val spark = SparkSession.builder().master("local").getOrCreate
//  import spark.implicits.given
//
//  println(summon[Encoder[Test0]].schema)
//  println(summon[Encoder[Test1]].schema)
//  println(summon[Encoder[Test2]].schema)
//  println(summon[Encoder[Test3]].schema)
//
//  spark.stop
