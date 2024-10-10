import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object JsonTest {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "karthik")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()


    val df1=spark.read
      .format("json")
      .option("multiline", true)
      .option("path","C:/Users/rahul/Downloads/oct10.json")
      .load()

    df1.show()

    val df3= df1.select (
      col("id"),
      col("name"),
      col("address.street").as("street"),
      col("address.city").as("city"),
      col("address.state").as("state"),
      explode(col("intrests")).as("intrests")
    ).select(
      col("id"),
      col("name"),
      col("street"),
      col("city"),
      col("state"),
      col("intrests.name").as("name"),
      col("intrests.category").as("category")
    ).show()


  }
}
