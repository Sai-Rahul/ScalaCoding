import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Ass1_complex_6 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_complex_4_Revision")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()


    import spark.implicits._

    val weather = Seq(
      (1, 25, 60),
      (2, 35, 40),
      (3, 15, 80)
    ).toDF("day_id", "temperature", "humidity")

    //How would you add two new columns, is_hot with values true if temperature is greater
    //than 30, and false otherwise, and is_humid with vales true if temp >50

    val df = weather.withColumn("is_hot",
        when(col("temperature") > 30, "true").otherwise("false"))
      .withColumn("is_humid", when(col("temperature") < 50, "true").otherwise("false"))

    df.show()

  }
}