import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object AverageScoreSql {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","AverageScoreSQL")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark= SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val scoreData = Seq(
      ("Alice", "Math", 80),
      ("Bob", "Math", 90),
      ("Alice", "Science", 70),
      ("Bob", "Science", 85),
      ("Alice", "English", 75),
      ("Bob", "English", 95)
    ).toDF("Student", "Subject", "Score")

    scoreData.createOrReplaceTempView("ScoreData")

    //spark.sql("SELECT * FROM ScoreData").show()

    //Finding the average score for each subject and the maximum score for each student

    //Finding the average score for each subject
    spark.sql(
      """
      SELECT Subject, AVG(Score) as Avg_score
      FROM ScoreData
      GROUP BY Subject
        """).show()


    //Finding the maximum score for each student
    spark.sql(
      """
        SELECT Student, MAX(Score) as Maximum_Score
        FROM ScoreData
        GROUP BY Student
        """).show()
  }

}
