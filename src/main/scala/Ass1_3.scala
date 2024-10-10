import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Ass1_3 {

  def main(args : Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","Ass1_3")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    //How would you add a new column category with values "High" if amount is greater than
    //1000, "Medium" if amount is between 500 and 1000, and "Low" otherwise?

    val transactions = List(
      (1, 1000),
      (2, 200),
      (3, 5000)
    ).toDF("transaction_id", "amount")

    val transaction_WithCategory = transactions.withColumn("Category",
      when(col("amount")> 1000,"High")
        .when(col("amount")> 500 && col("amount")< 1000,"Medium")
        .otherwise("low")
    )
    transaction_WithCategory.show()
  }

}
