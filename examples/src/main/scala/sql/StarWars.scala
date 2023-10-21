package sql

import buildinfo.BuildInfo.inputDirectory
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions.col
import scala3encoders.given
import scala3udf.{
  // "old" udf doesn't interfer with new scala3udf.udf when renamed
  Udf => udf
}
import scala.reflect.ClassTag

def map_udf[T](df: DataFrame, fn: udf)(implicit
    ct: ClassTag[T],
    enc: Encoder[T]
): Dataset[T] = {
  val columns = ct.runtimeClass.getDeclaredFields.map(_.getName).toList.map(col)
  df.select(fn(columns: _*).alias("udf")).select("udf.*").as[T]
}

case class Character(
    name: String,
    height: Double,
    weight: Option[Double],
    eyecolor: Option[String],
    haircolor: Option[String],
    jedi: Boolean,
    species: String
)

def toOption[T](what: String, parse: String => T) =
  if (what == "NA") None else Some(parse(what))

val character = udf(
  (
      name: String,
      height: Double,
      weight: String,
      eyecolor: String,
      haircolor: String,
      jedi: String,
      species: String
  ) =>
    Character(
      name,
      height,
      toOption(weight, _.toString.toDouble),
      toOption(eyecolor, _.toString),
      toOption(haircolor, _.toString),
      jedi == "jedi",
      species
    )
)

object StarWars extends App:
  val spark = SparkSession.builder().master("local").getOrCreate
  import spark.implicits._

  try
    case class Friends(name: String, friends: String)
    val friends: Dataset[Friends] = Seq(
      ("Yoda", "Obi-Wan Kenobi"),
      ("Anakin Skywalker", "Sheev Palpatine"),
      ("Luke Skywalker", "Han Solo, Leia Skywalker"),
      ("Leia Skywalker", "Obi-Wan Kenobi"),
      ("Sheev Palpatine", "Anakin Skywalker"),
      ("Han Solo", "Leia Skywalker, Luke Skywalker, Obi-Wan Kenobi, Chewbacca"),
      ("Obi-Wan Kenobi", "Yoda, Qui-Gon Jinn"),
      ("R2-D2", "C-3PO"),
      ("C-3PO", "R2-D2"),
      ("Darth Maul", "Sheev Palpatine"),
      ("Chewbacca", "Han Solo"),
      ("Lando Calrissian", "Han Solo"),
      ("Jabba", "Boba Fett")
    ).map(Friends.apply).toDS

    friends.show()
    case class FriendsMissing(who: String, friends: Option[String])
    val dsMissing: Dataset[FriendsMissing] = Seq(
      ("Yoda", Some("Obi-Wan Kenobi")),
      ("Anakin Skywalker", Some("Sheev Palpatine")),
      ("Luke Skywalker", Option.empty[String]),
      ("Leia Skywalker", Some("Obi-Wan Kenobi")),
      ("Sheev Palpatine", Some("Anakin Skywalker")),
      ("Han Solo", Some("Leia Skywalker, Luke Skywalker, Obi-Wan Kenobi"))
    ).map(FriendsMissing.apply).toDS

    dsMissing.show()

    val df = spark.sqlContext.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(s"${inputDirectory.getPath}/starwars.csv")

    val characters = map_udf[Character](df, character)
    characters.show()
    val sw_df = characters.join(friends, Seq("name"))
    sw_df.show()

    case class SW(
        name: String,
        height: Double,
        weight: Option[Double],
        eyecolor: Option[String],
        haircolor: Option[String],
        jedi: String,
        species: String,
        friends: String
    )

    val sw_ds = sw_df.as[SW]

  finally spark.close()
