package sql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._


object StarWars extends App:
  val spark = SparkSession.builder().master("local").getOrCreate
  import spark.implicits.localSeqToDatasetHolder
  import EncoderDerivation.given

  extension [T: Encoder] (seq: Seq[T])
    def toDS: Dataset[T] =
      localSeqToDatasetHolder(seq).toDS

  case class Friends(name: String, friends: String)
  val df: Dataset[Friends] = Seq(
      ("Yoda",             "Obi-Wan Kenobi"),
      ("Anakin Skywalker", "Sheev Palpatine"),
      ("Luke Skywalker",   "Han Solo, Leia Skywalker"),
      ("Leia Skywalker",   "Obi-Wan Kenobi"),
      ("Sheev Palpatine",  "Anakin Skywalker"),
      ("Han Solo",         "Leia Skywalker, Luke Skywalker, Obi-Wan Kenobi, Chewbacca"),
      ("Obi-Wan Kenobi",   "Yoda, Qui-Gon Jinn"),
      ("R2-D2",            "C-3PO"),
      ("C-3PO",            "R2-D2"),
      ("Darth Maul",       "Sheev Palpatine"),
      ("Chewbacca",        "Han Solo"),
      ("Lando Calrissian", "Han Solo"),
      ("Jabba",            "Boba Fett")
    ).toDS.map((n,f) => Friends(n, f))


  val friends = df.as[Friends]
  friends.show()
  case class FriendsMissing(who: String, friends: Option[String])
  val dsMissing: Dataset[FriendsMissing] = Seq( 
      ("Yoda",             Some("Obi-Wan Kenobi")),
      ("Anakin Skywalker", Some("Sheev Palpatine")),
      ("Luke Skywalker",   Option.empty[String]),
      ("Leia Skywalker",   Some("Obi-Wan Kenobi")),
      ("Sheev Palpatine",  Some("Anakin Skywalker")),
      ("Han Solo",         Some("Leia Skywalker, Luke Skywalker, Obi-Wan Kenobi"))
    ).toDS
     .map((a, b) => FriendsMissing(a, b)) 

  dsMissing.show()

  case class Character(
    name: String, 
    height: Integer, 
    weight: Option[Integer], 
    eyecolor: Option[String], 
    haircolor: Option[String], 
    jedi: String,
    species: String
  )

  val characters: Dataset[Character] = spark.sqlContext
    .read
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .csv("StarWars.csv")
    .as[Character]

  characters.show()
  val sw_df = characters.join(friends, Seq("name"))
  sw_df.show()

  case class SW(
    name: String,
    height: Integer,
    weight: Option[Integer],
    eyecolor: Option[String],
    haircolor: Option[String],
    jedi: String,
    species: String,
    friends: String
  )

  val sw_ds = sw_df.as[SW]

