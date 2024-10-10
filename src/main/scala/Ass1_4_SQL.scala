import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Ass1_4_SQL {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","Ass1_4_SQL")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor,memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    import spark.implicits._
    val products = List(
      (1, 30.5),
      (2, 150.75),
      (3, 75.25)
    ).toDF("product_id", "price")
    //How would you add a new column price_range with values "Cheap" if price is less than 50,
    //"Moderate" if price is between 50 and 100, and "Expensive" otherwise?

    products.createOrReplaceTempView("products")

    val resultDF = spark.sql(
      """
         SELECT product_id,price,
         CASE
         WHEN price <50 THEN "Cheap"
         WHEN price BETWEEN 50 and 100 THEN "Moderate"
         ELSE "Expensive"
         END as price_range
         FROM products



        """)

    resultDF.show()

  }

}
