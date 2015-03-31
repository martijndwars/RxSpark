import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import rx.lang.scala.Observable
import wrapper.Helper._

import scala.concurrent.duration._

object Main {
  def main(args: Array[String]): Unit = {
    // Create the context with a 1 second batch size. The "local[3]" means 3 threads.
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Clock")

    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Use local observable as input stream for Spark
    val clock = Observable.interval(100 milliseconds)
    val stream = RxUtils.createStream(ssc, clock)

    // Use output stream from Spark as observable
    stream
      .toObservable
      .subscribe(l => println("Observable says: " + l))
    
    ssc.start()
    ssc.awaitTermination()
  }
}
