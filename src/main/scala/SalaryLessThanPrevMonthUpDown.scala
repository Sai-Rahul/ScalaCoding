import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object SalaryLessThanPrevMonthUpDown {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)
    //   val spark=SparkSession.builder()
    //     .appName("Rahul")
    //     .master("local[*]")
    //     .getOrCreate()

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "DifferenceInPriceLeadLag")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val SalaryData = List(
      (1,"John",1000,"01/01/2016"),
      (1,"John",2000,"02/01/2016"),
      (1,"John",1000,"03/01/2016"),
      (1,"John",2000,"04/01/2016"),
      (1,"John",3000,"05/01/2016"),
      (1,"John",1000,"06/01/2016"))toDF("ID","NAME","SALARY","DATE")

        //SalaryData.show()

    //If salary is less than previous month we will mark it as "DOWN",
    // if salary has increased then "UP"

    val window= Window.orderBy("DATE")







  }
}