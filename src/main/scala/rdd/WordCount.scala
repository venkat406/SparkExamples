
package rdd

import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val input_path = "Data/wordCountExample.txt"
    val output_path = "out/wordCountResult"
    val conf = new SparkConf().setMaster("local").setAppName("Sample Word count Application")
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile(input_path)

    val counts = textRDD.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(output_path)

  }
}

