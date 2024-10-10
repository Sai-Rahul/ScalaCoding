import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, sum}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Orders {

  def main(args : Array[String]): Unit = {


    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)
    val sparkconf= new SparkConf()
    sparkconf.set("spark.app.name","Orders")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark= SparkSession.builder()
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

    //Finding the count of orders placed by each customer

    val ResultDF= orderData.groupBy(col("Customer")).agg(count(col("OrderID"))).show()

    // Finding the  total order amount for each customer
    val ResultDF2= orderData.groupBy(col("Customer")).agg(sum(col("Amount"))).show()

  }

}
