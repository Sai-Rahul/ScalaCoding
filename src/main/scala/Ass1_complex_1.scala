import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Ass1_complex_1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_complex_1")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val employees = List(
      (1, 25, 30000),
      (2, 45, 50000),
      (3, 35, 40000)
    ).toDF("employee_id", "age", "salary")

    //How would you add a new column category with values "Young & Low Salary" if age is less
    //than 30 and salary is less than 35000, "Middle Aged & Medium Salary" if age is between 30 and 40
    //and salary is between 35000 and 45000, and "Old & High Salary" otherwise?

    val df = employees.withColumn("category",
      when(col("age")< 30 && col("salary")< 35000,"Young & Low Salary")
        .when(col("age").between (30,40) && col("salary").between (35000,45000),"Middle Aged & Medium Salary")
        .otherwise("Old & High Salary")



    )
    df.show()


  }
}
