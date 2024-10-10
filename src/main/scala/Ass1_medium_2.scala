import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, when}

object Ass1_medium_2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_medium_2")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val customers = List(
      (1, "john@gmail.com"),
      (2, "jane@yahoo.com"),
      (3, "doe@hotmail.com")
    ).toDF("customer_id", "email")

    //How would you add a new column email_provider with values "Gmail" if email contains
    //"gmail", "Yahoo" if email contains "yahoo", and "Other" otherwise?

    val df = customers.withColumn("email_provider",
      when(col("email").contains("gmail"),"gmail")
        .when(col("email").contains("yahoo"),"yahoo")
        .otherwise("other")

    )
df.show()

  }
}
