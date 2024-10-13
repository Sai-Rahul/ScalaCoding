import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, datediff, to_date, when}

object Ass1_complex_4_Revision {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_complex_4_Revision")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()


    import spark.implicits._

    val tasks = List(
      (1, "2024-07-01", "2024-07-10"),
      (2, "2024-08-01", "2024-08-15"),
      (3, "2024-09-01", "2024-09-05")
    ).toDF("task_id", "start_date", "end_date")

 // How would you add a new column task_duration which is "Short" if the difference
    //between end_date and start_date is less than 7 days, "Medium" if it is between 7 and 14 days, and
    //"Long" otherwise

   //datediff Function: Use datediff() to calculate the difference between two dates.

    val df = tasks.withColumn("task_duration",
      when(datediff(to_date(col("end_date")),to_date(col("start_date"))) < 7,"short")
        .when(datediff(to_date(col("end_date")),to_date(col("start_date"))).between(7,14),"Medium")
        .otherwise("Long")
    )
   df.show()
  }
}
