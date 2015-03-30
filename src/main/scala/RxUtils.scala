import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}
import rx.lang.scala.{Observable, Subscription}

import scala.collection.mutable
import scala.reflect.ClassTag

object RxUtils {
  def createStream[T: ClassTag](ssc_ : StreamingContext, observable: Observable[T]): InputDStream[T] = {
    new RxInputDStream[T](ssc_, observable)
  }
}

// TODO: This class might be able to extend QueueInputDStream (less duplication). Think about BackPressure and such..
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
    subscription.map(_.unsubscribe())
  }

  override def compute(validTime: Time): Option[RDD[T]] = {
    // TODO: Currently, one at a time.
    if (storage.size > 0) {
      Some(ssc_.sparkContext.makeRDD(Seq(storage.dequeue()), 1))
    } else {
      None
    }
  }
}
