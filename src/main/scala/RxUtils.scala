import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}
import rx.lang.scala.{Subscriber, Observable, Subscription}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Reactive Extensions wrapper
 */
object RxUtils {
  def createStream[T: ClassTag](ssc_ : StreamingContext, observable: Observable[T]): InputDStream[T] = {
    new RxInputDStream[T](ssc_, observable)
  }

  def createBackpressuredStream[T: ClassTag](ssc_ : StreamingContext, observable: Observable[T]): InputDStream[T] = {
    new BackpressuredRxInputDStream[T](ssc_, observable)
  }
}

/**
 * Turn an observable into an InputDStream. The observable emits items which
 * are first put in a queue. Every batchDuration (as specified when creating
 * the StreamingContext) the compute() method is invoked. This constructs an
 * RDD from the queue contents, which is then returned.
 *
 * @param ssc_ Spark Streaming Context
 * @param observable Observable
 * @tparam T Type of items to emit
 */
class RxInputDStream[T: ClassTag](ssc_ : StreamingContext, observable: Observable[T]) extends InputDStream[T](ssc_) {
  var subscription: Option[Subscription] = None
  var storage: mutable.Queue[T] = new mutable.Queue[T]

  override def start() {
    subscription = Some(
      observable
        .subscribe(storage += _)
    )
  }

  override def stop() {
    subscription.foreach(_.unsubscribe())
  }

  override def compute(validTime: Time): Option[RDD[T]] = {
    if (storage.size > 0) {
      Some(ssc_.sparkContext.parallelize(storage.dequeueAll(_ => true)))
    } else {
      None
    }
  }
}



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

/**
 * Custom subscriber that requests more items only if
 *
 * @tparam T
 */
class BatchSubscriber[T] extends Subscriber[T] {
  var storage: mutable.Queue[T] = new mutable.Queue[T]
  var remaining: Int = 0
  var itemCount: Int = 1
  var halved: Boolean = false

  override def onStart(): Unit = {
    remaining = 1
    request(itemCount)
  }

  override def onNext(value: T): Unit = {
    remaining -= 1
    storage += value
  }

  /**
   * This method halves the number of requests under high load and doubles the
   * the number of requests under low load.
   *
   * @param idle Boolean indicating the load on the system
   */
  def pulse(idle: Boolean): Unit = {
    if (remaining == 0) {
      if (idle) {
        if (halved) {
          halved = false
        } else {
          itemCount *= 2
        }
        remaining = itemCount
        request(itemCount)
      } else {
        if (!halved) {
          itemCount /= 2
          halved = true
        }
      }
    }
  }
}
