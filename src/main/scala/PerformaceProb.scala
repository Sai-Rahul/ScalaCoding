import org.apache.spark
import org.apache.spark.sql.{SaveMode, SparkSession, functions => F}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

object PerformaceProb {

  def main(args: Array[String]): Unit = {

    val sparkconf= new SparkConf()
    sparkconf.set("spark.app.name","Rahul")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark= SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    //creating a df from list
    val data = List((1,"2024-01-10",8,"Good performance"),
      (2,"2024-01-15",9,"Excellent work!"),
      (3,"2024-02-20",6,"Needs improvement"),
      (4,"2024-02-25",7,"Good effort"),
      (5,"2024-03-05",10,"Outstanding"),
      (6,"2024-03-12",5,"Needs improvement")).toDF("employee_id","review_date","performance_score","review_text")


      val df = data.select(
        col("employee_id"),
        col("review_date"),
        col("performance_score"),
        col("review_text"),
        when(col("performance_score")>=9,"Excellent")
          .when(col("performance_score")>=7 && col("performance_score")< 9,"Good")
          .otherwise("Needs Improvement")
          .alias("performance_category"))
          df.filter(col("review_text")===("Excellent work!"))


        val df2= df.agg(avg(col("performance_score"))).show()

        //data.filter(col("review_text").contains("Excellent")).show()


     //data.createOrReplaceTempView("Performance")

//    spark.sql("""
//              SELECT employee_id,
//                     review_date,
//                     performance_score,
//                     avg(performance_score) as average_score,
//                     review_text,
//
//                     CASE
//                        WHEN performance_score > 9 THEN 'Excellent'
//                        WHEN performance_score BETWEEN 7 AND 9 THEN 'Good'
//                        ELSE 'Need Improvement'
//                     END AS performance_category
//              FROM Performance
//              WHERE review_text like '%Excellent%'
//              GROUP BY employee_id,review_date,performance_score,review_text
//
//
//    """).show()



  }

}
