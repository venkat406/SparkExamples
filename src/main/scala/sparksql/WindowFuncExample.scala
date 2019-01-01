package sparksql

import java.util.Date

import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.hadoop.fs.FileSystem
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions._

import scala.collection.immutable.HashMap
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat

import scala.util.Try
import org.apache.hadoop.fs.FileStatus
import java.util.GregorianCalendar
import java.util.Calendar
import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.catalyst.expressions.ParseToDate

object WindowFuncExample
{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Sample Test")
      .master("local").getOrCreate()
    import spark.implicits._
    val ds = Seq((1,"20180101",100),(2,"20180201",200),(3,"20180301",300),(4,"20180401",400),(5,"20181101",500)).toDF("sno","TIME_KEY","value")
    // val ds = spark.read.csv("C:\\Users\\duc\\Desktop\\Data\\Sample.csv")
    ds.show()


    var ds1 = ds.select(ds.col("*"),to_date($"TIME_KEY","yyyyMMdd").as("TIME_KEY_FMT"),
      when(month(to_date($"TIME_KEY","yyyyMMdd")) <= 1,lit(1)-month(to_date($"TIME_KEY","yyyyMMdd"))).otherwise(null).as("TIME_KEY01"),
      when(month(to_date($"TIME_KEY","yyyyMMdd")) <= 2,lit(2)-month(to_date($"TIME_KEY","yyyyMMdd"))).otherwise(null).as("TIME_KEY02"),
      when(month(to_date($"TIME_KEY","yyyyMMdd")) <= 3,lit(3)-month(to_date($"TIME_KEY","yyyyMMdd"))).otherwise(null).as("TIME_KEY03"),
      when(month(to_date($"TIME_KEY","yyyyMMdd")) <= 4,lit(4)-month(to_date($"TIME_KEY","yyyyMMdd"))).otherwise(null).as("TIME_KEY04"))



    var ds_result = ds1.select($"sno",$"TIME_KEY", $"value",expr("add_months(TIME_KEY_FMT,TIME_KEY01) AS TIME_KEY01"),
      expr("add_months(TIME_KEY_FMT,TIME_KEY02) AS TIME_KEY02"),
      expr("add_months(TIME_KEY_FMT,TIME_KEY03) AS TIME_KEY03"),
      expr("add_months(TIME_KEY_FMT,TIME_KEY04) AS TIME_KEY04"))

    ds_result.show()
    val MAX_DATE = spark.sql("SELECT MAX(date_format('2018-09-19','yyyyMMdd')) AS MAX_DATE").collectAsList.get(0).get(0).toString().toLong
    print(MAX_DATE)
    ds_result = ds_result.filter("TIME_KEY_FMT <= " + "to_date(\""+MAX_DATE+"\",\"yyyyMMdd\")")
    ds_result.show()

    val time_key_01_part = Window.partitionBy("TIME_KEY01").orderBy($"TIME_KEY".desc)
    val time_key_02_part = Window.partitionBy("TIME_KEY02").orderBy($"TIME_KEY".desc)
    val time_key_03_part = Window.partitionBy("TIME_KEY03").orderBy($"TIME_KEY".desc)
    val time_key_04_part = Window.partitionBy("TIME_KEY04").orderBy($"TIME_KEY".desc)


    var agg_results = ds_result.withColumn("AGG_VALUE_01",sum(when($"TIME_KEY01".isNotNull,$"value").otherwise(null)) over time_key_01_part)
      .withColumn("AGG_VALUE_02",sum(when($"TIME_KEY02".isNotNull,$"value").otherwise(null))  over time_key_02_part)
      .withColumn("AGG_VALUE_03",sum(when($"TIME_KEY03".isNotNull,$"value").otherwise(null))  over time_key_03_part)
      .withColumn("AGG_VALUE_04",sum(when($"TIME_KEY04".isNotNull,$"value").otherwise(null))  over time_key_04_part)
    agg_results.show()
  }
}



