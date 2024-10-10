import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Ass1_medium_1_SQL {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_medium_1_SQL")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val inventory = List(
      (1, 5),
      (2, 15),
      (3, 25)
    ).toDF("item_id", "quantity")

    //How would you add a new column stock_level with values "Low" if quantity is less than 10,
    //"Medium" if quantity is between 10 and 20, and "High" otherwise?

    inventory.createOrReplaceTempView("inventory")

    val resultDF = spark.sql(
      """
         SELECT item_id,quantity,
         CASE
         WHEN quantity < 10 THEN "Low"
         WHEN quantity BETWEEN 10 AND 20 THEN "Medium"
         ELSE "High"
         END AS stock_level
         FROM inventory


        """)

    resultDF.show()

  }
}
