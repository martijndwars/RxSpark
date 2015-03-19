import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import rx.lang.scala.Observable
import wrapper.Helper._

import scala.concurrent.duration._

object InputTest {

  def main(args: Array[String]): Unit = {
    // Create the context with a 1 second batch size. The "local[3]" means 3 threads.
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("IntervalObs")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val obs = Observable.interval(1 seconds)

    val stream = obs.toDStream(ssc)
    stream.foreachRDD(x => x.foreach(println))

    ssc.start()
    ssc.awaitTermination()
  }

}
