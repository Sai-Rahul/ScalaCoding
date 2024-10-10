import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object CountOfOccurence {

  def main(args : Array[String]): Unit = {

    val sparkconf= new SparkConf()
    sparkconf.set("spark.app.name","CountOfOccurence")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val textData = Seq(
      "Hello, how are you?",
      "I am fine, thank you!",
      "How about you?"
    ).toDF("Text")

    textData.show(false)

    // Split the text into words
    val words = textData.select(explode(split($"Text","\\s+")).alias("word"))

    // Group by Word and calculate the count

    val wordsCount= words.groupBy("word").agg(count(col("word"))).alias("count").show()
  }

}
