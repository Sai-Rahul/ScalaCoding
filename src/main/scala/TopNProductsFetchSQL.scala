import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TopNProductsFetchSQL {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "TopNProductsFetchSQL")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor,memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val salesData = Seq(
      ("Product1", 100),
      ("Product2", 200),
      ("Product3", 150),
      ("Product4", 300),
      ("Product5", 250),
      ("Product6", 180)
    ).toDF("Product", "SalesAmount")

    salesData.createOrReplaceTempView("salesData")

    //spark.sql("select * from salesData").show()

    //Find the top N products with the highest sales amount

    spark.sql(
      """
         SELECT Product,
         SalesAmount
         FROM salesData
         ORDER BY SalesAmount
         DESC


        """)


  }
}

