import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Ass1_complex_5_SQL {

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
    val orders = List(
      (1, 5, 100),
      (2, 10, 150),
      (3, 20, 300)
    ).toDF("order_id", "quantity", "total_price")

    // How would you add a new column order_type with values "Small & Cheap" if quantity is
    //less than 10 and total_price is less than 200, "Bulk & Discounted" if quantity is greater than or equal
    //to 10 and total_price is less than 200, and "Premium Order" otherwise

    orders.createOrReplaceTempView("orders")

    val resultdf = spark.sql(
      """
         SELECT order_id, quantity, total_price,
         CASE
         WHEN quantity <10 AND total_price < 200 THEN "Small & Cheap"
         WHEN quantity >=10 AND total_price < 200 THEN "Bulk & Discounted"
         ELSE "Premium Order"
         END AS order_type
         FROM orders

        """)

    resultdf.show()


  }
}
