import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, date_add, date_sub, lit, to_date, to_timestamp, when}

object DateManipulationFunctions {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)
    //   val spark=SparkSession.builder()
    //     .appName("Rahul")
    //     .master("local[*]")
    //     .getOrCreate()

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "DateMissingManupulations")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val df = List(("2023-10-07","15:30:00"))toDF("date_str","time_str")

    df.printSchema()
    val formattedDf= df.withColumn("date",to_date($"date_str"))
    .withColumn("time", to_timestamp($"time_str"))

    formattedDf.show()
    formattedDf.printSchema()
    val df2= df.withColumn("future_date",date_add(col("date_str"),50)).show()

    //Past days of 50
    val df3= df.withColumn("Past_date",date_sub(col("date_str"),50)).show()


    val DF= Seq(("2023-10-07",null),(null,"2023-10-08")).toDF("date1","date2")

    DF.withColumn("date1",when(col("date1").isNull, lit("01-01-2024")).otherwise(col("date1")))
      .withColumn("date2",coalesce(col("date2"),lit("NA"))).show()



  }
}