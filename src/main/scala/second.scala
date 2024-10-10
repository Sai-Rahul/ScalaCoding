import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object second {
  def main(args: Array[String]): Unit = {

    val sparkconf= new SparkConf()
    sparkconf.set("spark.app.Name","Rahul")
    sparkconf.set("spark.master","local[*]")

    val spark= SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header",true)
      .option("path","C:/Users/rahul/Downloads/details.csv")
      .load()

//   df.show()
//    df.select(col("id"),col("Salary")).show()
    df.select(
      col("id"),
      col("Salary"),
      when(col("Salary")>=500,"RICH")
      .otherwise("POOR")
        .alias("Status")
    ).show()

  }
}
