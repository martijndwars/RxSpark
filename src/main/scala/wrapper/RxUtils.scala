package wrapper

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import rx.lang.scala.Observable

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
