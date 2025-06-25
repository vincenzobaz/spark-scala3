package udf

// Generates the apply methods used in Udf.scala
// actual examples: see UdfSpec.scala
object Gen extends App:

  import java.io.PrintWriter

  var content = ""
  (0 to 22).foreach { i =>
    val is = (1 to i).toList
    val ts = is.map(x => s"T${x}")
    val types = (ts :+ "R").mkString(", ")
    val lines =
      s"def apply[${types}](" ::
        s"    f: Function${i}[${types}]" ::
        ")(using" ::
        "    er: AgnosticEncoder[R]," ::
        ("    dr: Deserializer[R]" + (if i > 0 then "," else "")) ::
        (is
          .map(x => s"    et${x}: AgnosticEncoder[T${x}]")
          .mkString(",\n")) ::
        "): Udf =" ::
        "  createUdf(" ::
        "    f," ::
        "    dr.inputType," ::
        (if (i == 0) then "    Seq(),"
         else if (i == 1) then "    Seq(Some(summon[AgnosticEncoder[T1]])),"
         else s"    summonSeq[(${ts.mkString(",")})],") ::
        "    Some(summon[AgnosticEncoder[R]])," ::
        "    None" ::
        "  )" :: Nil

    val tab = "  "
    content += tab + lines.mkString("\n" + tab) + "\n\n"
  }

  new PrintWriter("Gen.scala.log") { write(content); close }
