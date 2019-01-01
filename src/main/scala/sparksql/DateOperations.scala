package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DateOperations
{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Sample Test")
      .master("local").getOrCreate()
    import spark.implicits._
    val ds = Seq((1, "20180101", 100), (2, "20180201", 200), (3, "20180301", 300), (4, "20180401", 400), (5, "20181101", 500)).toDF("sno", "TIME_KEY", "value")
    ds.show()

    var ds1 = ds.select(ds.col("*"), to_date($"TIME_KEY", "yyyyMMdd").as("TIME_KEY_FMT"),
      when(month(to_date($"TIME_KEY", "yyyyMMdd")) <= 1, lit(1) - month(to_date($"TIME_KEY", "yyyyMMdd"))).otherwise(null).as("TIME_KEY01"),
      when(month(to_date($"TIME_KEY", "yyyyMMdd")) <= 2, lit(2) - month(to_date($"TIME_KEY", "yyyyMMdd"))).otherwise(null).as("TIME_KEY02"),
      when(month(to_date($"TIME_KEY", "yyyyMMdd")) <= 3, lit(3) - month(to_date($"TIME_KEY", "yyyyMMdd"))).otherwise(null).as("TIME_KEY03"),
      when(month(to_date($"TIME_KEY", "yyyyMMdd")) <= 4, lit(4) - month(to_date($"TIME_KEY", "yyyyMMdd"))).otherwise(null).as("TIME_KEY04"))


    var ds_result = ds1.select($"sno", $"TIME_KEY", $"value", expr("add_months(TIME_KEY_FMT,TIME_KEY01) AS TIME_KEY01"),
      expr("add_months(TIME_KEY_FMT,TIME_KEY02) AS TIME_KEY02"),
      expr("add_months(TIME_KEY_FMT,TIME_KEY03) AS TIME_KEY03"),
      expr("add_months(TIME_KEY_FMT,TIME_KEY04) AS TIME_KEY04")).drop("TIME_KEY01", "TIME_KEY02")

    ds_result.show()
  }
}
