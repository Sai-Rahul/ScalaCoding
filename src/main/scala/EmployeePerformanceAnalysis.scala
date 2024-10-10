import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, lag, month, sum, to_date, when}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window

object EmployeePerformanceAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val sparkconf= new SparkConf()
    sparkconf.set("spark.app.name","EmployeePerformanceAnalysis")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val EmployeePerformanceAnalysis = List(
      ("E001","Sales",85,"2024-02-10","Sales Manager"),
      ("E002","HR",78,"2024-03-15" ,"HR Assistant"),
      ("E003","IT",92,"2024-01-22","IT Manager"),
      ("E004","Sales",88,"2024-02-18","Sales Rep"),
      ("E005","HR",95,"2024-03-20","HR Manager")
    )toDF("employee_id","department","performance_score","review_date","position")
    EmployeePerformanceAnalysis.show()
    EmployeePerformanceAnalysis.printSchema()

    //Create a new column review_month using month extracted from review_date.
    //convert the string time date format to yyyy-MM-dd
    val dfDate = EmployeePerformanceAnalysis.withColumn("review_date",to_date($"review_date", "yyyy-MM-dd"))

    dfDate.printSchema()

    val dfMonth = dfDate.withColumn("review_month",month($"review_date"))
    dfMonth.show()

    //Filter records where the position ends with 'Manager' and the performance_score is greater than 80.

    val FilteredRecord = dfMonth.filter($"position".endsWith("Manager")&& $"performance_score"> 80)
    FilteredRecord.show()

    //Group by department and review_month, and calculate:
    //The average performance_score per department per month.
    //The count of employees who received a performance_score above 90.

    // Step 3: Group by department and review_month
    val groupedDF = dfMonth
      .groupBy($"department", $"review_month")
      .agg(
        avg($"performance_score").alias("avg_performance_score"),
        sum(when($"performance_score" > 90, 1).otherwise(0)).alias("count_above_90"))

  groupedDF.show()

 //Use the lag function to calculate the performance improvement or decline for each employee compared to their previous review

    val windowSpec = Window.partitionBy("employee_id").orderBy("review_date")

    val dfWithLag = dfMonth
      .withColumn("previous_performance_score", lag($"performance_score", 1).over(windowSpec))
      .withColumn("performance_change", $"performance_score" - $"previous_performance_score")

    dfWithLag.show()


  }

}
