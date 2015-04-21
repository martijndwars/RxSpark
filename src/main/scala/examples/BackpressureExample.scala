package examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import rx.lang.scala.Observable
import wrapper.RxUtils
import wrapper.Helper._

object BackpressureExample {
  def main(args: Array[String]) = {
    // Create the context with a 1 second batch size. The "local[3]" means 3 threads.
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Clock")
      .set("spark.ui.showConsoleProgress", "false")

    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create an observable that supports backpressure
    val ticker = Observable.from(0 to 10000)
    val stream = RxUtils.createBackpressuredStream(ssc, ticker)

    // Simulate a slow stream so jobs will start piling up
    val slowStream = stream
      .map(x => heavyComputation(x))

    // Use output stream from Spark as observable
    slowStream
      .toObservable
      .subscribe(l => l + 1)
    //.subscribe(l => println("Observable says: " + l))

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Simulate heavy computation so jobs start piling up
   */
  def heavyComputation(x: Int) = {
    Thread.sleep(100)
    x
  }
}
