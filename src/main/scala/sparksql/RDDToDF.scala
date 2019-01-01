package sparksql

import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RDDToDF {
  case class Example(sno:String, message:String) //un typed data types
  def main(args: Array[String]): Unit = {
    val inputPath = "Data/exampleDataset.txt"
    val conf = new SparkConf().setMaster("local").setAppName("RDD to DataFrame")
    val sc = new SparkContext(conf)  // with this we will get RDDs on data
    val spark = SparkSession
      .builder()
      .appName("Spark SQL example")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val textRDD = sc.textFile(inputPath)
    textRDD.take(10)
    val exampleRDD = textRDD.map { line => val col = line.split(",")
      Example(col(0),col(1))
    }
    val exampleDF = exampleRDD.toDF()
    exampleDF.show()

  }
}

