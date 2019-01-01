
import org.apache.spark.sql._
object SQLExample
{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SQL Connection")
      .master("local")
      .getOrCreate()
    val jdbcURL = "jdbc:sqlserver://DESKTOP-K6MQVHP:1433;databaseName=master;"
    val sqlData = spark.sqlContext.read.format("jdbc").option("url",jdbcURL)
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("user","venka")
      .option("password","password")
      .option("dbtable","sample1").load()
    val results = sqlData.filter("sno == 1")
    results.show()
  }
}
