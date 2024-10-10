import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Ass1_2 {

  def main(args: Array[String]): Unit = {

    val sparkconf= new SparkConf()
    sparkconf.set("spark.app.name","Ass1_2")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val grades = List(
      (1, 85),
      (2, 42),
      (3, 73)
    ).toDF("student_id","score")

    //How would you add a new column grade with values "Pass" if score is greater than or equal to 50,
    // and "Fail" otherwise?

    val df = grades.withColumn("grade_withvalues",
      when(col("score")>=50, "true")
      .otherwise("fail")

    )

    df.show()
  }

}
