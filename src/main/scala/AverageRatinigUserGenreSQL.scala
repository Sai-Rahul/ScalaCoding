import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AverageRatinigUserGenreSQL {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "AverageRatingUserGenreSQL")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val ratingData = Seq(
      ("User1", "Movie1", "Action", 4.5),
      ("User1", "Movie2", "Drama", 3.5),
      ("User1", "Movie3", "Comedy", 2.5),
      ("User2", "Movie1", "Action", 3.0),
      ("User2", "Movie2", "Drama", 4.0),
      ("User2", "Movie3", "Comedy", 5.0),
      ("User3", "Movie1", "Action", 5.0),
      ("User3", "Movie2", "Drama", 4.5),
      ("User3", "Movie3", "Comedy", 3.0)
    ).toDF("User", "Movie", "Genre", "Rating")

    ratingData.createOrReplaceTempView("ratingData")

    spark.sql(
      """
         SELECT User,
         Genre,
         AVG(Rating) AS Average_Rating
         FROM ratingData
         GROUP BY User,Genre

        """).show()


  }
}
