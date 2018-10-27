import org.apache.spark.sql.DataFrame

def preapreSQL(data_frame: DataFrame, table_name: String): String = {
  return "select "
    .concat(data_frame.columns.filter(_ != "address")
      .toSeq
      .sorted
      .mkString(", "))
    .concat(" , ")
    .concat(Seq("address.address1", "address.city", "address.state")
      .sorted
      .mkString(", "))
    .concat(" from ")
    .concat(table_name);
}

def lookDiff(data_frame1: DataFrame, data_frame2: DataFrame): Long = {
  var total_diff: Long = 0
  if (data_frame1.columns.sameElements(data_frame2.columns)) {
    println("columns are matching now comparing data...")
    println("checking for difference in files...")
    println(data_frame1.inputFiles.mkString(",\n"))
    println("against ")
    println(data_frame2.inputFiles.mkString(",\n"))
    val df1_df2_df = data_frame1.except(data_frame2)
    total_diff = df1_df2_df.count
    if (total_diff > 0) {
      println("differences on files...")
      df1_df2_df.show
      return total_diff
    }
    else {
      println("data matches.....")
    }
  }
  else {
    val columns = data_frame1.schema.fields.map(_.name)
    val selectiveDifferences = columns.map(col => data_frame1.select(col).except(data_frame2.select(col)))
    selectiveDifferences.map(diff => {
      total_diff = diff.count
      if (total_diff > 0) {
        diff.show
        return total_diff
      }
    })
  }
  return total_diff
}

def comapreDF(sourceFile :String , destinationFile : String) : Unit = {
  val df1: DataFrame = spark.read.json(sourceFile)
  val df2: DataFrame = spark.read.json(destinationFile)
  df1.registerTempTable("employee1")
  df2.registerTempTable("employee2")

  val sqlQueryEmployee1 = preapreSQL(df1, "employee1")
  val sqlQueryEmployee2 = preapreSQL(df2, "employee2")
  val emp_df1 = spark.sql(sqlQueryEmployee1)
  val emp_df2 = spark.sql(sqlQueryEmployee2)

  if (0 == (lookDiff(emp_df1, emp_df2) + lookDiff(emp_df2, emp_df1))) {
    println("both files match")
  }
  else {
    throw new IllegalArgumentException("files do not match")
  }
}

comapreDF("employee1.json", "employee2.json")
System.exit(0)


