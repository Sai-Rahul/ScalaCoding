import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case object MaxRevenueProductWindowPartition {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)
    //   val spark=SparkSession.builder()
    //     .appName("karthik")
    //     .master("local[*]")
    //     .getOrCreate()

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "karthik")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val salesData = Seq(
      ("Product1", "Category1", 100),
      ("Product2", "Category2", 200),
      ("Product3", "Category1", 150),
      ("Product4", "Category3", 300),
      ("Product5", "Category2", 250),
      ("Product6", "Category3", 180)
    ).toDF("Product", "Category", "Revenue")

    //Finding the maximum revenue for each product's category and the corresponding product.
    //find the max revenue for each product
    val window = Window.partitionBy("Category")
    val df = salesData.withColumn("Maximum_revenue",max(col("Revenue")).over(window))
      df.show()

    val window1 = Window.partitionBy("Product")
    val df1 = salesData.withColumn("Maximum_revenue",max(col("Revenue")).over(window1))
    df1.show()



  }
}
