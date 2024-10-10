import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, when}
import org.apache.spark.sql.types.DateType

object Ass1_medium_3_Revision {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_medium_3_Revision")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val orders = List(
      (1, "2024-07-01"),
      (2, "2024-12-01"),
      (3, "2024-05-01")
    ).toDF("order_id", "order_date")

    //How would you add a new column season with values "Summer" if order_date is in June,
    //July, or August, "Winter" if in December, January, or February, and "Other" otherwise

    //Step1: First convert the orderdate from String to Date Type

    val orderswithdate = orders.withColumn("order_date",col("order_date").cast(DateType))

    //step2: add the season column

    val orderswithSeason = orderswithdate.withColumn("season",

      when(month(col("order_date")).isin(6,7,8),"summer")
        .when(month(col("order_date")).isin(1,2,12),"winter")
        .otherwise("other")
    )

    orderswithSeason.show()

  }
}