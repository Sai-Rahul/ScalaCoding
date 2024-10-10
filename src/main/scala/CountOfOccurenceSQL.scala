import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CountOfOccurenceSQL {

  def main(args : Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","CountOfOccurenceSQL")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark= SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()


    import spark.implicits._
    val textData = Seq(
      "Hello, how are you?",
      "I am fine, thank you!",
      "How about you?"
    ).toDF("Text")

    textData.show(false)
    textData.createOrReplaceTempView("textData")

    // Split the text into words
    spark.sql(
      """
         SELECT
         explode(split(Text, '\\s+')) as word
         FROM textData

        """).show()

    // Create a new temporary view with the exploded words
    textData.createOrReplaceTempView("wordsData")

    // Group by Word and calculate the count
    spark.sql(
      """
     SELECT Word,
     COUNT(Word) AS count
     FROM wordsData
     GROUP BY Word
    """
    ).show()
  }

}
