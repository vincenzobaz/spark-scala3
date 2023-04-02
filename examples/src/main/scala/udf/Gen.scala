package udf

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
        "    er: ExpressionEncoder[R]," ::
        ("    dr: Deserializer[R]" + (if i > 0 then "," else "")) ::
        (is
          .map(x => s"    et${x}: ExpressionEncoder[T${x}]")
          .mkString(",\n")) ::
        "): Udf =" ::
        "  create_udf(" ::
        "    f," ::
        "    dr.inputType," ::
        (if (i == 0) then "    Seq(),"
          else if (i == 1) then "    Seq(Some(summon[ExpressionEncoder[T1]])),"
          else s"    summon_seq[(${ts.mkString(",")})],") ::
        "    Some(summon[ExpressionEncoder[R]])," ::
        "    None" ::
        "  )" :: Nil

    val tab = "  "
    content += tab + lines.mkString("\n" + tab) + "\n\n"
  }  

  new PrintWriter("out.log") { write(content); close }


  /*
    Seq(false, true).foreach { withRegister =>
    (0 to 22).foreach { i =>
      val is = (1 to i).toList
      val ts = is.map(x => s"T${x}")
      val types = (ts :+ "R").mkString(", ")
      val name = if (withRegister) then "udf3_register" else "udf3"
      val lines =
        s"def ${name}[${types}](" ::
          (if (withRegister) then
             s"    f: Function${i}[${types}],\n" +
               "    spark: SparkSession,\n" +
               "    name: String"
           else s"    f: Function${i}[${types}]") ::
          ")(using" ::
          "    er: ExpressionEncoder[R]," ::
          ("    sr: Serializer[R]" + (if i > 0 then "," else "")) ::
          (is
            .map(x => s"    et${x}: ExpressionEncoder[T${x}]")
            .mkString(",\n")) ::
          "): UserDefinedFunction =" ::
          (if (withRegister) then "  val res = create_function("
           else "  create_function(") ::
          "    f," ::
          "    sr.inputType," ::
          (if (i == 0) then "    Seq(),"
           else if (i == 1) then "    Seq(Some(summon[ExpressionEncoder[T1]])),"
           else s"    summon_seq[(${ts.mkString(",")})],") ::
          "    Some(summon[ExpressionEncoder[R]])," ::
          (if (withRegister) then "    Some(name)" else "    None") ::
          "  )" ::
          (if (withRegister) then "  internal_register(spark, name, f, res)\n"
           else "") :: Nil

      content += lines.mkString("\n") + "\n"
*/