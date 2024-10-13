import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Ass1_complex_9_SQL {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_complex_8")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val payments = List(
      (1, "2024-07-15"),
      (2, "2024-12-25"),
      (3, "2024-11-01")
    ).toDF("payment_id", "payment_date")

    //How would you add a new column quarter with values "Q1" if payment_date is in January,
    //February, or March, "Q2" if in April, May, or June, "Q3" if in July, August, or September, and "Q4"
    //otherwise?
    payments.createOrReplaceTempView("payments")

    val resultdf = spark.sql(
      """
         SELECT payment_id,payment_date,
         CASE
         WHEN MONTH(payment_date) IN (1,2,3) THEN "Q1"
         WHEN MONTH(payment_date) IN (4,5,6) THEN "Q2"
         WHEN MONTH(payment_date) IN (7,8,9) THEN "Q3"
         ELSE "Q4"
         END AS quarter
         FROM payments

        """)

    resultdf.show()


  }
}
