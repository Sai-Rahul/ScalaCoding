import org.apache.spark
import org.apache.spark.sql.{SaveMode, SparkSession, functions => F}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object WindowPartitionTestCase {


    def main(args: Array[String]): Unit = {

      Logger.getLogger(("org")).setLevel(Level.OFF)
      Logger.getLogger(("akka")).setLevel(Level.OFF)
      //   val spark=SparkSession.builder()
      //     .appName("karthik")
      //     .master("local[*]")
      //     .getOrCreate()

      val sparkconf=new SparkConf()
      sparkconf.set("spark.app.Name","karthik")
      sparkconf.set("spark.master","local[*]")
      sparkconf.set("spark.executor.memory","2g")

      val spark=SparkSession.builder()
        .config( sparkconf)
        .getOrCreate()



      //  val df=spark.read.csv("C:/Users/Karthik Kondpak/Documents/details.csv")


      //    val ddlschema="id Int,Name String,Salary Int,City String"

      //    val pgschema=StructType(List(
      //      StructField("id",IntegerType),
      //      StructField("Name",StringType),
      //      StructField("Salary",IntegerType),
      //      StructField("City",StringType)
      //    ))
      //
      //    val df=spark.read
      //      .format("csv")
      //      .option("header",true)
      //      .schema(pgschema)
      //      .option("path","C:/Users/Karthik Kondpak/Documents/details.csv")
      //      .load()
      //
      //    df.show()
      //
      //    df.printSchema()
      //
      //
      //    df.write
      //      .format("csv")
      //      .option("header",true)
      //      .mode(SaveMode.Ignore)
      //      .option("path","C:/Users/Karthik Kondpak/Documents/sep26op")


      import spark.implicits._
      val salesData = Seq(
        ("Product1", "Category1", 100),
        ("Product2", "Category2", 200),
        ("Product3", "Category1", 150),
        ("Product4", "Category3", 300),
        ("Product5", "Category2", 250),
        ("Product6", "Category3", 180),
        ("Product1", "Category2", 100),
        ("Product2", "Category1", 200),
        ("Product3", "Category2", 150),
        ("Product4", "Category4", 300),
        ("Product5", "Category1", 250),
        ("Product6", "Category4", 180)
      ).toDF("Product", "Category", "Revenue")



      val windowSpec = Window.partitionBy("Product").orderBy("Category")

      val runningTotal = salesData.withColumn("RunningTotal", avg("Revenue").over(windowSpec))
      runningTotal.show()





      //      .save()



      //   df.show(2,false)

      //   df.select(col("id"),column("Salary")).show()

      import spark.implicits._

      //    val data=List((1, "Smartphone", 700 "Electronics"),("vijay",45,23),("ajay",67,68)).toDF("name","age","marks")
      //
      //    product_id product_name price category
      //      1 Smartphone 700 Electronics
      //      2 TV 1200 Electronics
      //      3 Shoes 150 Apparel
      //      4 Socks 25 Apparel
      //      5 Laptop 800 Electronics
      //      6 Jacket 200 Apparel


      //    val data = List(
      //    (1, "Smartphone", 700, "Electronics"),
      //    (2, "TV", 1200, "Electronics"),
      //    (3, "Shoes", 150, "Apparel"),
      //    (4, "Socks", 25, "Apparel"),
      //    (5, "Laptop", 800, "Electronics"),
      //    (6, "jacket", 200,"Apparel")
      //    ).toDF("product_id","product_name","price", "category")
      //
      //
      //  data.groupBy(col("product_id")) .agg(sum("price")).show()




















      //   val df1=data.select(col("product_id"),col("price")
      //     ,when(col("price")>500,"EXPENSIVE")
      //      .when(col("price")>200 and col("price")>=500,"Moderate")
      //       .otherwise("cheap")
      //   ).show()
      //
      //    data.filter(col("product_name").startsWith("S")).show()
      //    data.filter(col("product_name").endsWith("s")).show()
      //
      //  data.groupBy("category").agg(sum(col("price")),avg(col("price")),max(col("price")),min(col("price"))).show()
      ////
      //   data.createOrReplaceTempView("customer")
      //
      //    spark.sql("""
      //    SELECT
      //         product_id,
      //         price,
      //         case
      //         when price>500 then "Expensive"
      //         when price between 200 and 500 then "moderate"
      //         else "cheap"
      //         end
      //    FROM customer
      //""").show()
      //
      //    spark.sql("""
      //    SELECT
      //         *
      //         FROM customer where product_name like  "S%"
      //""").show()
      //
      //    spark.sql("""
      //    SELECT
      //         *
      //
      //    FROM customer where product_name like "%s"
      //""").show()
      //
      //
      //    spark.sql("""
      //    SELECT
      //         category,
      //         sum(price),
      //         avg(price),
      //         max(price),
      //         min(price)
      //    FROM customer group by category
      //""").show()
      //
      ////   data.withColumn("status",when(col("age")>55 && col("name").endsWith("n"),"senior").otherwise("junior")).show()

      scala.io.StdIn.readLine()

      //    data.select(
      //       col("name")
      //        ,col("age")
      //        ,col("marks"),
      //        when(col("age")>55 && col("name").endsWith("n"),"senior").otherwise("junior").alias("status")
      //      ).show()

      //    data.filter(col("age")>55 && col("name").startsWith("m")).show()
    }


}
