import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Ass_1_SQL {

  def main(args: Array[String]) : Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","Ass_1_SQL")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val employees = List(
      (1, "John", 28),
      (2, "Jane", 35),
      (3, "Doe", 22)
    ).toDF("id","name","age")

    employees.createOrReplaceTempView("employees")

    val resultDF = spark.sql(
      """
         SELECT id, name,age,
         CASE
         WHEN age >= 18 THEN "true" ELSE "false"
         END As Is_Adult

         FROM employees
        """)
    resultDF.show()
  }

}
