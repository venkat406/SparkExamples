package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object UDFExample
{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UDF Example")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val ds = Seq((1,"20180101",100),(2,"20180201",200),(3,"20180301",300),(4,"20180401",400),(5,"20181101",500)).toDF("sno","TIME_KEY","value")

    def createRanges : (Int => String) =  {value =>
      var range = "> 1000"
      if ( value <= 1000)
        {
          range = "0 to 1000"
        }
      range
    }
    val createRange = udf(createRanges)
    val ds1 = ds.withColumn("rangeCol",createRange($"value"))
    ds1.show()
  }
}
