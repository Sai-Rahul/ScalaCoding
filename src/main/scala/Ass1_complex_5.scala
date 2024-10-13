import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Ass1_complex_5 {

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

    val df = orders.withColumn("order_type",
      when(col("quantity")<10 && col("total_price") < 200,"Small & Cheap")
        .when(col("quantity")>= 10 && col("total_price")<200,"Bulk & Discounted")
        .otherwise("Premium Order")


    )

df.show()
  }
}
