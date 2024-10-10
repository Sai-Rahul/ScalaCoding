import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object AverageRating {

  def main(args : Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkconf= new SparkConf()
       sparkconf.set("spark.app.name","AverageMovieRating")
       sparkconf.set("spark.master","local[*]")
       sparkconf.set("spark.executor.memory","2g")

    val spark= SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val ratingsData= Seq(

      ("User1", "Movie1", 4.5),
      ("User2", "Movie1", 3.5),
      ("User3", "Movie2", 2.5),
      ("User4", "Movie2", 3.0),
      ("User1", "Movie3", 5.0),
      ("User2", "Movie3", 4.0)
    ).toDF("User", "Movie", "Rating")

    //ratingsData.show()
    // Group by Movie and calculate the average rating

    val DF= ratingsData.groupBy("Movie").agg(avg(col("Rating"))).show()

    //// Group by Movie and calculate the count of ratings

    val DF2= ratingsData.groupBy("Movie").agg(count(col("Rating"))).show()
  }

}
