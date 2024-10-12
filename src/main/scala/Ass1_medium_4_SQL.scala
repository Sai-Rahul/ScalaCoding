import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level,Logger}

object Ass1_medium_4_SQL {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","Ass1_medium_4_SQL")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val sales = List(
      (1, 100),
      (2, 1500),
      (3, 300)
    ).toDF("sale_id", "amount")

    sales.createOrReplaceTempView("sales")

    // How would you add a new column discount with values 0 if amount is less than 200, 10 if
    //amount is between 200 and 1000, and 20 if amount is greater than 1000?

    val resultDF = spark.sql(
      """
         SELECT sale_id,amount,
         CASE
         WHEN amount < 200 THEN "0"
         WHEN amount BETWEEN 200 AND 1000 THEN "10"
         ELSE "20"
         END AS discount
         FROM sales



       """)

    resultDF.show()
  }

}
