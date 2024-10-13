import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Ass1_complex_8_SQL {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_complex_8_SQL")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val emails = List(
      (1, "user@gmail.com"),
      (2, "admin@yahoo.com"),
      (3, "info@hotmail.com")
    ).toDF("email_id", "email_address")

    //How would you add a new column email_domain with values "Gmail" if email_address
    //contains "gmail", "Yahoo" if it contains "yahoo", and "Hotmail" otherwise?
    emails.createOrReplaceTempView("emails")

    val resultdf = spark.sql(
      """
         SELECT email_id, email_address,
         CASE
         WHEN email_address LIKE '%gmail%' THEN "GMAIL"
         WHEN email_address LIKE '%Yahoo%' THEN "YAHOO"
         ELSE "Hotmail"
        END AS email_domain
        FROM emails

        """)

    resultdf.show()



  }
}
