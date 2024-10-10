import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object First {

  def main(args: Array[String]): Unit = {


      val sparkconf=new SparkConf()
      sparkconf.set("spark.app.Name","Rahul")
      sparkconf.set("spark.master","local[*]")
      sparkconf.set("spark.executor.memory","2g")

      val spark=SparkSession.builder()
        .config( sparkconf)
        .getOrCreate()



      val df1=spark.read
      .format("json")
      .option("multiline", true)
      .option("path","C:/Users/rahul/Downloads/oct9.json")
      .load()

    df1.show()

  }

}
