import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Ass1_medium_5_SQL {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_medium_5")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val logins = List(
      (1, "09:00"),
      (2, "18:30"),
      (3, "14:00")
    ).toDF("login_id", "login_time")

    //How would you add a new column is_morning which is true if login_time is before 12:00,
    //and false otherwise

    logins.createOrReplaceTempView("logins")

    val resultdf = spark.sql(
      """
         SELECT login_id, login_time,
         CASE
         WHEN login_time < '12:00' THEN true
         ELSE false
         END AS is_morning
         FROM logins
        """)

    resultdf.show()
  }
}

