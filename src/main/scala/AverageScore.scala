import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max}

object AverageScore {

  def main(args: Array[String]): Unit ={

    val sparkconf= new SparkConf()
    sparkconf.set("spark.app.name","averagescore")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
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

    scoreData.groupBy(col("Subject")).agg(avg("Score")).show()
    scoreData.groupBy(col("Student")).agg(max("Score")).show()
  }

}
