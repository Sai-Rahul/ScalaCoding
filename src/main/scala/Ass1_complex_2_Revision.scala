import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Ass1_complex_2_Revision {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_complex_1_SQL")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val reviews = List(
      (1, 1),
      (2, 4),
      (3, 5)
    ).toDF("review_id", "rating")

    //: How would you add two new columns, feedback with values "Bad" if rating is less than 3,
    //"Good" if rating is 3 or 4, and "Excellent" if rating is 5, and is_positive with values true if rating is
    //greater than or equal to 3, and false otherwise

    val reviewsWithFeedback  = reviews.withColumn("feedback with values",
      when(col("rating") < 3,"Bad")
        .when(col("rating").between(3,4),"Good")
        .otherwise("excellent")
    )

    val finalDF = reviewsWithFeedback.withColumn("is_positive",
      (col("rating") >= 3)

    )

    finalDF.show()


  }
}