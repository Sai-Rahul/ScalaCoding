import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Ass1_complex_7 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_complex_7")
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

    val df = scores.withColumn("math_grade",
      when(col("math_score") >80, "A")
        .when(col("math_score").between(60,80),"B")
        .otherwise("C"))
        .withColumn("english_grade",
          when(col("english_score")>80 , "A")
            .when(col("english_score").between(60,80),"B")
            .otherwise("C")
        )

    df.show()




  }
}
