import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
object CustomerPurchase {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf= new SparkConf()
    sparkconf.set("spark.app.name","CustomerPurchase")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val purchaseData = Seq(
      ("Customer1", "Product1", 100),
      ("Customer1", "Product2", 150),
      ("Customer1", "Product3", 200),
      ("Customer2", "Product2", 120),
      ("Customer2", "Product3", 180),
      ("Customer3", "Product1", 80),
      ("Customer3", "Product3", 250)
    ).toDF("Customer", "Product", "Amount")

    purchaseData.show()

    //Finding the count of distinct products purchased by each customer and the total purchase
    //amount for each customer

    //Finding the count of distinct products purchased by each customer

    val ResultDF= purchaseData.groupBy("Customer").agg(countDistinct(col("Product"))).alias("DistinctCountProducts")

    //Group by Customer and calculate the sum of Amount

    val resultDF2= purchaseData.groupBy("Customer").agg(sum(col("Amount"))).alias("SumOfAmount")

    ResultDF.show()
    resultDF2.show()

  }

}
