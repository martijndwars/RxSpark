import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}
import rx.lang.scala.{Observable, Subscription}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Reactive Extensions wrapper
 */
object RxUtils {
  def createStream[T: ClassTag](ssc_ : StreamingContext, observable: Observable[T]): InputDStream[T] = {
    new RxInputDStream[T](ssc_, observable)
  }
}

/**
 * Turn an observable into an InputDStream. Items emitted from the observable
 * are queued until they are pulled by Spark.
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
