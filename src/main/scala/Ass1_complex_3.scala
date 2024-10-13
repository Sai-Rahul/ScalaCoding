import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Ass1_complex_3 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ass1_complex_3")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val documents = List(
      (1, "The quick brown fox"),
      (2, "Lorem ipsum dolor sit amet"),
      (3, "Spark is a unified analytics engine")
    ).toDF("doc_id", "content")

    //How would you add a new column content_category with values "Animal Related" if
    //content contains "fox", "Placeholder Text" if content contains "Lorem", and "Tech Related" if content
    //contains "Spark"

    val df = documents.withColumn("content_category",
      when(col("content").contains("fox"),"Animal Related")
        .when(col("content").contains("Lorem"),"Placeholder Text")
        .otherwise("Spark")

    )

    df.show()


  }
}
