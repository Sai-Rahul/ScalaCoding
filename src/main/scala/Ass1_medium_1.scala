import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Ass1_medium_1 {
  def main(args: Array[String]) : Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","Ass1_medium_1")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

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

    val df = inventory.withColumn("stock_level",
      when(col("quantity") < 10, "Low")
        .when(col("quantity")>10 && col("quantity") <20, "Medium")
        .otherwise("High")

    )
     df.show()

  }
}

