package examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import rx.lang.scala.Observable
import twitter4j.{Status, StatusAdapter, TwitterStreamFactory}
import wrapper.RxUtils
import wrapper.Helper._
import scala.concurrent.duration._

object TwitterExample {
  val CLEAR_CONSOLE = "\u001b[2J"

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

    // Create the context with a 1 second batch size. The "local[3]" means 3 threads.
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("TwitterExample")
      .set("spark.ui.showConsoleProgress", "false")

    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create and operate on streams
    val stream = RxUtils.createStream(ssc, tweets)

    val tagsStream = stream
      .flatMap(_.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = tagsStream
      .map((_, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // Use output as observable
    topCounts60
      .toObservable
      .slidingBuffer(1 seconds, 1 seconds)
      .subscribe(topList => {
        // Jump to bottom, so we can actually see what happens
        println(CLEAR_CONSOLE)

        // Print popular tags
        topList
          .take(10)
          .foreach(tag => println(tag))
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
