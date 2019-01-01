package sparksql


//import org.apache.spark.SparkConf
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkSQLExample {
  // schema definition
  case class Example(sno: String, message: String)
  def main(args: Array[String]) {

    val inputPath = "Data/jsonExample.json"
    val spark = SparkSession
      .builder()
      .appName("Spark SQL example")
      .master("local")
      .getOrCreate()
    // import the implicits function to support sql operations such as lit, etc.,.
    import spark.implicits._

    runExample(spark, inputPath)
    spark.stop()
  }

  private def runExample(spark: SparkSession, inputPath:String): Unit = {
    val df = spark.read.json(inputPath)
    // Displays the content of the DataFrame to stdout
    df.show()

    import spark.implicits._
    // Print the schema in a tree format
    df.printSchema()

    // Select only the "message" column
    df.select("message").show()

    // Select records with sno > 1
    df.select($"sno", $"message" ).show()

    df.filter($"sno" > 1).show()

    // Count records by sno
    df.groupBy("sno").count().show()


    // Register the DataFrame as a SQL temporary view
    var ds = df.withColumn("sno_new",$"sno")
    ds = ds.withColumn("sno_new1",lit(10))
    // ds = ds.withColumnRenamed("sno","sno_renamed")
    ds = ds.filter("sno > 1")
    ds = ds.filter($"sno" > 1)

    val ds2 = ds.select("sno", "message");
    val ds3 = ds2.where("sno > 1")
    val ds4 = ds3.where($"sno" > 1)

    ds2.show()
    ds3.show()
    ds4.show()
    ds2.describe("sno")

    val ds7 = ds.join(ds2, "sno").drop(ds2("sno"))
    val ds8 = ds.join(ds2, ds("sno") === ds2("sno")).drop(ds2("sno"))

    val ds9 = ds.join(ds2, ds("sno") === ds2("sno"),"right").drop(ds2("sno"))

    val ds10 = ds2.union(ds3).union(ds4)

    val ds11 = ds.select("sno").as("sno alias")

    val ds13 = ds.sort($"sno".desc)
    val ds14 = ds.orderBy($"sno".asc)
    val ds15 = ds.selectExpr("sno","message as message_new")
    val ds16 = ds.select(expr("sno"), expr("message as message_new"))
    // convert into DataFrame
    val df1 = ds16.toDF()

    df.createOrReplaceTempView("exampleDF")

    val sqlDF = spark.sql("SELECT * FROM exampleDF")
    sqlDF.show()

    // $example on:global_temp_view$
    // Register the DataFrame as a global temporary view
    df.createGlobalTempView("exampleDF")

    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.exampleDF").show()

    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.exampleDF").show()

  }
}


