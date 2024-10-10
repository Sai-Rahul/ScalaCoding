import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Ass1_3_SQL {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","Ass1_3_SQL")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val transactions = List(
      (1, 1000),
      (2, 200),
      (3, 5000)
    ).toDF("transaction_id", "amount")

    transactions.createOrReplaceTempView("transactions")

    //How would you add a new column category with values "High" if amount is greater than
    //1000, "Medium" if amount is between 500 and 1000, and "Low" otherwise?

    val resultDF = spark.sql(
      """
         SELECT transaction_id,amount,
         CASE
         WHEN amount > 1000 THEN "High"
         WHEN amount  BETWEEN 500 AND 1000 THEN "Medium"
         ELSE "Low"
         END as Category
         FROM transactions

        """)

    resultDF.show()



  }

}
