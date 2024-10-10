import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object OrdersSQL {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val sparkconf= new SparkConf()
    sparkconf.set("spark.app.name","orders")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val orderData = Seq(
      ("Order1", "John", 100),
      ("Order2", "Alice", 200),
      ("Order3", "Bob", 150),
      ("Order4", "Alice", 300),
      ("Order5", "Bob", 250),
      ("Order6", "John", 400)
    ).toDF("OrderID", "Customer", "Amount")

    orderData.createOrReplaceTempView("orderData")

    // Run a SQL query to display the data
    //spark.sql("select * from orderData ").show()

    //Finding the count of orders placed by each customer

    spark.sql(
      """
         SELECT Customer, COUNT("OrderID")
         FROM orderData
         GROUP BY Customer
        """).show()

    // Finding the  total order amount for each customer

    spark.sql(
      """
         SELECT Customer, SUM(Amount)
         FROM orderData
         GROUP BY customer

        """).show()


  }

}
