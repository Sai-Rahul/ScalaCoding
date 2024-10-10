import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Ass1_medium_3_SQL_Revision {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_medium_3_SQL_Revision")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val orders = List(
      (1, "2024-07-01"),
      (2, "2024-12-01"),
      (3, "2024-05-01")
    ).toDF("order_id", "order_date")

    orders.createOrReplaceTempView("orders")

    //How would you add a new column season with values "Summer" if order_date is in June,
    //July, or August, "Winter" if in December, January, or February, and "Other" otherwise

    val resultDF = spark.sql(
      """
         SELECT order_id, order_date,
         CASE
         WHEN MONTH(order_date) IN (6,7,8) THEN "SUMMER"
         WHEN MONTH(order_date) IN (1,2,12) THEN "WINTER"
         ELSE "OTHER"
         END as Season
         FROM orders


        """)

    resultDF.show()

  }
}
