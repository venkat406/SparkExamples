import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object MySQLExample
{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MySQL Connection")
      .master("local")
      .getOrCreate()
    val mysqlData = spark.sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/practice")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("dbtable","sample1")
      .option("user","root")
      .option("password","password").load()
    val results = mysqlData.filter("sno == 1")
    results.show()
  }
}