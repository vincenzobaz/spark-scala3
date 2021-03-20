package sql
//
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoder

@main def encoderMine =
  val spark = SparkSession.builder().master("local").getOrCreate
  import EncoderDerivation.given

  println(summon[Encoder[Test0]].schema)
  println(summon[Encoder[Test1]].schema)
  println(summon[Encoder[Test2]].schema)
  println(summon[Encoder[Test3]].schema)

  spark.stop

