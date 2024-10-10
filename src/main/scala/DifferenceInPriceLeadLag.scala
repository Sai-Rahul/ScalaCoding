import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}
object DifferenceInPriceLeadLag {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)
    //   val spark=SparkSession.builder()
    //     .appName("Rahul")
    //     .master("local[*]")
    //     .getOrCreate()

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "DifferenceInPriceLeadLag")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val data = List(
      (1,"KitKat",1000.0,"2021-01-01"),
      (1,"KitKat",2000.0,"2021-01-02"),
      (1,"KitKat",1000.0,"2021-01-03"),
      (1,"KitKat",2000.0,"2021-01-04"),
      (1,"KitKat",3000.0,"2021-01-05"),
       (1,"KitKat",1000.0,"2021-01-06")
    )toDF("IT_ID","IT_NAME","Price","Price_Date")

    //data.show()

    //find the difference between the price on each day with it’s previous day

    val windowSpec = Window.orderBy("Price_Date")
    val df = data.select(
      col("IT_ID"),
      col("IT_NAME"),
      col("Price"),
      col("Price_Date"),
      col("Price")-lag(col("Price"),1)
        .over(windowSpec)

    ).show()

  }
}
