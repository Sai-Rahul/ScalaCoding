import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Ass1_complex_7_SQL {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_complex_7_SQL")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val scores = List(
      (1, 85, 92),
      (2, 58, 76),
      (3, 72, 64)
    ).toDF("student_id", "math_score", "english_score")

    //How would you add two new columns, math_grade with values "A" if math_score is
    //greater than 80, "B" if it is between 60 and 80, and "C" otherwise, and english_grade with values "A"
    //if english_score is greater than 80, "B" if it is between 60 and 80, and "C" otherwise?

    scores.createOrReplaceTempView("scores")
    val resultdf = spark.sql(
      """
         SELECT student_id,math_score,english_score,
         CASE
         WHEN math_score >80 THEN "A"
         WHEN math_score BETWEEN 60 AND 80 THEN "B"
         ELSE "C"
         END AS math_grade,
         CASE
         WHEN english_score >80 THEN "A"
         WHEN english_score BETWEEN 60 AND 80 THEN "B"
         ELSE "C"
         END AS english_grade
         FROM scores




        """)

    resultdf.show()


  }
}
