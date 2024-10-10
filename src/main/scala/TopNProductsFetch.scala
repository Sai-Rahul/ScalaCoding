import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TopNProductsFetch {

  def main(args : Array[String]): Unit = {

    val sparkconf= new SparkConf()
    sparkconf.set("spark.app.name","TopNProductsFetch")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor,memory","2g")

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
    salesData.show()

    //Find the top N products with the highest sales amount

    val ResultDf= salesData.orderBy($"SalesAmount".desc).limit(2)

    ResultDf.show()

  }

}
