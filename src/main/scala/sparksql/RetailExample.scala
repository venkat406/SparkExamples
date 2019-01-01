package sparksql
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object RetailExample
{
  def main(args: Array[String]): Unit = {
    // Get the retail dataset from Kaggle [https://www.kaggle.com/sanjeet41/online-retail]
    val inputPath = "C:\\Users\\venka\\Documents\\Dataset\\online_retail.csv"
    val spark = SparkSession.builder().appName("Retail Example")
      .master("local")
      .getOrCreate()

    // read csv file with header and as Dataset
    val salesData = spark.read.format("csv").option("header","true").load(inputPath)
    // Task 1 : Create a new column sales = Quantity * UnitPrice
    val salesResult = salesData.withColumn("sales",col("Quantity") * col("UnitPrice"))
    salesResult.show()

    // Task 2 : Calculate aggregates MAX , MIN, Average sales per country from the result
    val salesAggs = salesResult.groupBy("Country").agg(max("sales").as("Max_sales"),
      min("sales").as("min_sales"),
      avg("sales").as("avg_sales"))
    salesAggs.show()

    // Task 3 : parse Date and create 3 columns for year, month, day columns
    var salesDate = salesResult.withColumn("InvoiceDateFormatted",to_date(col("InvoiceDate"),"MM/dd/yy"))
    salesDate = salesDate.select(salesResult.col("*"), year(col("InvoiceDateFormatted")).as("InvoiceDate_year"),
      month(col("InvoiceDateFormatted")).as("InvoiceDate_month"),
      dayofmonth(col("InvoiceDateFormatted")).as("InvoiceDate_day"))
    salesDate.show()
    val outputPath = "out/sales.parquet"
    salesDate.write.parquet(outputPath)

  }
}
