import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead}

object LeadOrderDate {
  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)
    //   val spark=SparkSession.builder()
    //     .appName("karthik")
    //     .master("local[*]")
    //     .getOrCreate()

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "LeadOrderDate")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val OrdersData= List(
      (101,"CustomerA","2023-09-01"),
      (103,"CustomerA","2023-09-03"),
      (102,"CustomerB","2023-09-02"),
      (104,"CustomerB","2023-09-04")
    )toDF("Order_Id","Customer","Order_Date")

    //apply lead on top of order_date......and get the answer
    val window = Window.orderBy("Customer")
    val df = OrdersData.select(col("Order_Id"),col("Customer"),
      col("Order_Date"),lead(col("Order_Date"),1)
        .over(window)).show()



  }

}
