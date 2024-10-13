import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Ass1_complex_6_SQL {
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
 weather.createOrReplaceTempView("weather")
     val resultdf= spark.sql(
       """
          SELECT day_id,temperature,humidity,
          CASE
          WHEN temperature >30 THEN 'true'
          ELSE 'false'
          END AS is_hot,
          CASE
          WHEN temperature < 50 THEN 'true'
          ELSE 'false'
          END AS is_humid
          FROM weather



        """)

        resultdf.show()
}
}
