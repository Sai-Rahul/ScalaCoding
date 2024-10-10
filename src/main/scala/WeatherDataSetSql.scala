import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WeatherDataSetSql {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "WeatherDataSetsql")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val weatherData = Seq(
      ("City1", "2022-01-01", 10.0),
      ("City1", "2022-01-02", 8.5),
      ("City1", "2022-01-03", 12.3),
      ("City2", "2022-01-01", 15.2),
      ("City2", "2022-01-02", 14.1),
      ("City2", "2022-01-03", 16.8)
    ).toDF("City", "Date", "Temperature")


  }

}
