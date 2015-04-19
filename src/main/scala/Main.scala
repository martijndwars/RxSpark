import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import rx.lang.scala.Observable
import wrapper.Helper._

object Main {
  def main(args: Array[String]): Unit = {
    // Create the context with a 1 second batch size. The "local[3]" means 3 threads.
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("Clock")

    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Use local observable as input stream for Spark
    //val nonBackpressureableClock = Observable.interval(100 milliseconds)
    //val stream = RxUtils.createStream(ssc, nonBackpressureableClock)

    val backpressureableClock = Observable.from(0 to 100)
    val stream = RxUtils.createBackpressuredStream(ssc, backpressureableClock)

    // Simulate a slow stream so jobs will start piling up
    val slowStream = stream
      .map(_ + 1)
    //.map(_ => Thread.sleep(3000))

    // Use output stream from Spark as observable
    slowStream
      .toObservable
      .subscribe(l => println("Observable says: " + l))

    ssc.start()
    ssc.awaitTermination()
  }
}
