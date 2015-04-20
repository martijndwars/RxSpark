package wrapper

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}
import rx.lang.scala.{Observable, Subscription}

import scala.reflect.ClassTag

/**
 * An RxInputDStream that uses backpressure to prevent the obserable from
 * emitting more than Spark can handle.
 *
 * The size of the JobSet queue in the JobScheduler is used as an indicator of
 * the workload. Items are only emitted if the queue is below a threshold.
 *
 * @see https://github.com/ReactiveX/RxJava/wiki/Backpressure
 * @param ssc_ Spark Streaming Context
 * @param observable Observable
 * @tparam T Type of items to emit
 */
class BackpressuredRxInputDStream[T: ClassTag](ssc_ : StreamingContext, observable: Observable[T]) extends InputDStream[T](ssc_) {
  var subscription: Option[Subscription] = None
  var subscriber: Option[BatchSubscriber[T]] = None

  override def start(): Unit = {
    subscriber = Some(new BatchSubscriber[T])

    subscription = Some(
      observable
        .subscribe(subscriber.get)
    )
  }

  override def stop(): Unit = {
    subscription.foreach(_.unsubscribe())
  }

  override def compute(validTime: Time): Option[RDD[T]] = {
    // Turn storage queue into an RDD
    val rdd = if (subscriber.get.storage.size > 0) {
      Some(ssc_.sparkContext.parallelize(subscriber.get.storage.dequeueAll(_ => true)))
    } else {
      None
    }

    subscriber.get.pulse(ssc_.getScheduler().getJobSets().isEmpty)

    rdd
  }
}
