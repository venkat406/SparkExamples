package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object StreamingExample {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Streaming Word Count")

    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created

    val lines = ssc.textFileStream("hdfs_or_(remote)_file_path")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}



