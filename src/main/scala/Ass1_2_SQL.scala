import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Ass1_2_SQL {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","Ass1_2_SQL")
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

    grades.createOrReplaceTempView("grades")

    val resultDF = spark.sql(
      """
         SELECT student_id, score,
         CASE
         WHEN score >= 50 then "pass" ELSE "fail"
         END As grade_with_values

         FROM grades

        """)

    resultDF.show()
  }

}
