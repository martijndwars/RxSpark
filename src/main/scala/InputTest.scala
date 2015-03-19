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
    val yoloObs = obs.map(x => new Yolo(x.toString))

    // Turn our observable into a DStream
    val stream = yoloObs.toDStream(ssc)

    // Let our workers do heavy computations on our Yolo objects
    val stream2 = stream.map(y => y.heavyComputation())

    // Get back the results and print those
    stream2.foreachRDD(x => x.foreach(println))

    ssc.start()
    ssc.awaitTermination()
  }

}

class Yolo(val number: String) extends Serializable {
  def heavyComputation() = {
    Thread.sleep(3)
    
    new Integer(number) * 2
  }
}
