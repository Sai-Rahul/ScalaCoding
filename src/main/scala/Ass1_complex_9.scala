import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, when}
import org.apache.spark.sql.types.DateType

object Ass1_complex_9 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_complex_8")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val payments = List(
      (1, "2024-07-15"),
      (2, "2024-12-25"),
      (3, "2024-11-01")
    ).toDF("payment_id", "payment_date")

    //How would you add a new column quarter with values "Q1" if payment_date is in January,
    //February, or March, "Q2" if in April, May, or June, "Q3" if in July, August, or September, and "Q4"
    //otherwise?

   //convert payment date to datetype

    val payment_dateType = payments.withColumn("payment_date",col("payment_date").cast(DateType))

    val df = payment_dateType.withColumn("quarter",
      when(month(col("payment_date")).isin(1,2,3),"Q1")
        .when(month(col("payment_date")).isin(4,5,6),"Q2")
        .when(month(col("payment_date")).isin(7,8,9),"Q3")
        .otherwise("Q4")
    )
 df.show()



  }
}
