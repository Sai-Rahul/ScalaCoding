import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead, max}

object LeadLagExample {




    def main(args: Array[String]): Unit = {

      Logger.getLogger(("org")).setLevel(Level.OFF)
      Logger.getLogger(("akka")).setLevel(Level.OFF)
      //   val spark=SparkSession.builder()
      //     .appName("karthik")
      //     .master("local[*]")
      //     .getOrCreate()

      val sparkconf = new SparkConf()
      sparkconf.set("spark.app.Name", "LeadLag")
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

      //Lead and Lag Functinalities



      val window = Window.orderBy("Revenue")
      val df = salesData.select(col("Product"),col("Revenue"),lead(col("Revenue"),1).over(window))
      df.show()

      val df2 = salesData.select(col("Product"),col("Revenue"),lead(col("Revenue"),1).over(window))
      df2.show()




    }

  }

