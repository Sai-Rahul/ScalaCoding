import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object AttendanceProb {

  def main(args : Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","Rahul")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark= SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()


    import spark.implicits._
    val data =  List((1,"HR",25,"2026-03"),
      (2,"IT",18,"2026-03"),
      (3,"Sales",7,"2026-03"),
      (4,"IT",20,"2026-03"),
      (5,"HR",22,"2026-03"),
      (6,"Sales",12,"2026-03")).toDF("employee_id","department","attendance_days","attendance_month")

//    val df = data.select(
//      col("employee_id"),
//      col("department"),
//      col("attendance_days"),
//      col("attendance_month"),
//      when(col("attendance_days") >= 22,"Excellent")
//        .when(col("attendance_days") >=15 && col("attendance_days") <22,"Good")
//        .when(col("attendance_days")>=8 && col("attendance_days")<15,"Average")
//        .otherwise("Poor")
//        .alias("attendance_category")).show()

    data.createOrReplaceTempView("Attendance")

    spark.sql(
      """
        SELECT employee_id,
              department,
              attendance_days,
              attendance_month,

              CASE
                WHEN attendance_days > 22 THEN 'Excellent'
                WHEN attendance_days BETWEEN 15 AND 22 THEN 'Good'
                WHEN attendance_days BETWEEN 8 AND 15 THEN 'AVERAGE'
                ELSE 'POOR'
              END AS attendance_category
              FROM Attendance

        """).show()










  }

}
