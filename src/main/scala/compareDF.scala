import org.apache.spark.sql.DataFrame
import scala.collection.immutable.Map

def prepareSql(data_frame: DataFrame, table_name: String): String = {
  data_frame.registerTempTable(table_name)
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
    .concat(table_name)
}

def createMap(data_frame: DataFrame): Map[String, Any] = {
  data_frame.show
  return data_frame.columns.toList zip data_frame.collect.head.toSeq toMap
}

def lookDiff(data_frame1: DataFrame, data_frame2: DataFrame): Long = {
  if (data_frame1.columns.sameElements(data_frame2.columns)) {
    println("columns are matching now comparing data...")
    println("checking for difference in files...")
    println(data_frame1.inputFiles.mkString(","))
    println("against ")
    println(data_frame2.inputFiles.mkString(","))
    data_frame1.select("id").collect.foreach(row => {
      val id: Long = row.getLong(0)
      println("comparing data for Id:" + id)
      val map1 = createMap(data_frame1.filter("id = " + id))
      val map2 = createMap(data_frame2.filter("id = " + id))
      println("Creted map1 " + map1)
      println("Creted map2 " + map2)
      val diff1_2 = (map1.toSet diff map2.toSet).toMap
      val diff2_1 = (map2.toSet diff map1.toSet).toMap
      if (diff1_2 != Set.empty || diff2_1 != Set.empty) {
        println("differences from 1 -> 2 " + diff1_2)
        println("differences from 2 -> 1 " + diff2_1)
        return -1
      }
      else {
        println("records are good")
      }
      println("***********completed comparing data for Id:" + id)
    })
  }
  else {
    println("checking for difference in files...")
    println(data_frame1.inputFiles.mkString(","))
    println("against ")
    println(data_frame2.inputFiles.mkString(","))
    val diff_1_2 = data_frame1.columns.toSet.diff(data_frame2.columns.toSet)
    if (diff_1_2.size > 0) {
      println("missing fields...[" + diff_1_2.mkString(", ") + "]")
      return -1
    }
  }
  println("*****************************iteration compiled***************************************")
  return 0;
}

def comapreDF(sourceFile: String, destinationFile: String): Unit = {
  val df1: DataFrame = spark.read.json(sourceFile)
  val df2: DataFrame = spark.read.json(destinationFile)
  df1.registerTempTable("employee1")
  df2.registerTempTable("employee2")

  val sqlQueryEmployee1 = prepareSql(df1, "employee1")
  val sqlQueryEmployee2 = prepareSql(df2, "employee2")
  val emp_df1 = spark.sql(sqlQueryEmployee1)
  val emp_df2 = spark.sql(sqlQueryEmployee2)

  if (0 == (lookDiff(emp_df1, emp_df2) + lookDiff(emp_df2, emp_df1))) {
    println("both files match")
  }
  else {
    throw new IllegalArgumentException("files do not match")
  }
}

comapreDF("resources/employee1.json", "resources/employee3.json")
System.exit(0)


