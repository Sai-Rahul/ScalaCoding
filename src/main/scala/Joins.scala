import org.apache.spark.sql.SparkSession

object Joins {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Rahul")
      .master("local[*]")
      .getOrCreate()

    val df1 = spark.read
      .format("csv")
      .option("header",true)
      .option("path","C:/Users/rahul/Downloads/details2.csv")
      .load()


    val df2 = spark.read
      .format("csv")
      .option("header",true)
      .option("path","C:/Users/rahul/Downloads/info.csv")
      .load()


    val condition = df1("id")===df2("id")

    val jointype = "fullouter"

    val joineddf = df1.join(df2,condition,jointype).drop(df2("id"))

    joineddf.show()
  }

}
