import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, sum}


object Practise {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "Practise")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()


    //Finding the count of orders placed by each customer and the total order amount for
    //each customer
import spark.implicits._
    // Create the DataFrame
    val orderDF = Seq(
      ("Order1", "John", 100),
      ("Order2", "Alice", 200),
      ("Order3", "Bob", 150),
      ("Order4", "Alice", 300),
      ("Order5", "Bob", 250),
      ("Order6", "John", 400)
    ).toDF("OrderID", "Customer", "Amount")

     val ResultDF= orderDF.groupBy("customer")
      .agg(count("OrderID"),sum("Amount")).show()

  }
}
