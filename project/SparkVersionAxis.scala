import sbt._

case class SparkVersionAxis(idSuffix: String, directorySuffix: String)
    extends VirtualAxis.WeakAxis {}
