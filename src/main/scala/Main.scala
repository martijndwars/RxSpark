import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import Helper._

object Main {
  def main(args: Array[String]): Unit = {
    // Create the context with a 1 second batch size. The "local[3]" means 3 threads.
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create a socket stream on target ip:port and count the
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    // This seems to work! Apparently, collect() sends the RDD back or someth.?
    wordCounts
      .toObservable
      .subscribe(l => println("Observable says: " + l))

    ssc.start()
    ssc.awaitTermination()
  }
}
