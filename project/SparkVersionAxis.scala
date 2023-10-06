import sbt._

case class SparkVersionAxis(
    idSuffix: String,
    directorySuffix: String,
    sparkVersion: String
) extends VirtualAxis.WeakAxis {}
