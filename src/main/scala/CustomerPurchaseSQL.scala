import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger,Level}

object CustomerPurchaseSQL {

  def main(args : Array[String]): Unit = {

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


    purchaseData.createOrReplaceTempView("purchaseData")

    //Finding the count of distinct products purchased by each customer

    spark.sql(
      """SELECT Customer,
         COUNT (DISTINCT Product) as Distinct_Count
         FROM purchaseData
         GROUP BY Customer



        """).show()


    //Group by Customer and calculate the sum of Amount
    spark.sql(
      """SELECT Customer,
         SUM(Amount) as Amount_Sum
         FROM purchaseData
         GROUP BY Customer


        """).show()

  }

}
