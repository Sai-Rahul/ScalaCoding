import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Ass1_1 {

  def main(args: Array[String]) : Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Assignment1_1")
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

   //How would you add a new column is_adult which is true if the age is greater than or equal
    //to 18, and false otherwise

    val df = employees.withColumn("is_adult",
      when(col("age")>=18 , "true")
        .otherwise("false")
    )

    df.show()
  }
}
