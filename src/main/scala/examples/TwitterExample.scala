package examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import rx.lang.scala.Observable
import twitter4j.{Status, StatusAdapter, TwitterStreamFactory}
import wrapper.RxUtils
import wrapper.Helper._

object TwitterExample {
  def main(args: Array[String]): Unit = {
    // Create observable of tweets
    val tweets = Observable[Status](subscriber => {
      val twitterStream = new TwitterStreamFactory().getInstance()

      twitterStream.addListener(new StatusAdapter {
        override def onStatus(status: Status): Unit =
          subscriber.onNext(status)

        override def onException(e: Exception): Unit =
          subscriber.onError(e)
      })

      twitterStream.sample()
    })

    // Instantiate Spark cluster
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("TwitterExample")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val stream = RxUtils.createStream(ssc, tweets)
    val textStream = stream
      .map(_.getText) // TODO: Do something more interesting?

    // Use output as observable
    textStream
      .toObservable
      .subscribe(t => println(t))

    ssc.start()
    ssc.awaitTermination()
  }
}
