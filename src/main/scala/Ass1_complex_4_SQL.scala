import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Ass1_complex_4_SQL {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_complex_4_SQL")
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

    tasks.createOrReplaceTempView("tasks")

    val resultdf = spark.sql(
      """ SELECT task_id,start_date,end_date,
          CASE
          WHEN DATEDIFF(TO_DATE(end_date), TO_DATE(start_date)) < 7 THEN "Short"
          WHEN DATEDIFF(TO_DATE(end_date),TO_DATE(start_date)) BETWEEN 7 AND 14 THEN "Medium"
          ELSE "LONG"
          END AS task_duration
          FROM tasks



        """)

    resultdf.show()


  }
}
