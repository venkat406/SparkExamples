package sparksql
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ParquetToAvro
{
  def main(args: Array[String]): Unit = {
    val inputPath = "out/sales.parquet"
    val outputPath = "out/sales.avro"
    val spark = SparkSession.builder().appName("Parquet to Avro")
      .master("local")
      .getOrCreate()

    val parquetSales = spark.read.parquet(inputPath).toDF()

    val results = parquetSales.filter("Country == \"United Kingdom\"").filter("InvoiceNo == 536365")
    results.show()
    results.write.format("avro").save(outputPath)
  }
}
