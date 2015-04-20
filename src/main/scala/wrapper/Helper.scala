package wrapper

import org.apache.spark.streaming.dstream.DStream
import rx.lang.scala.Observable

/**
 * Helper to make `.toObservable` available as an extension method on a
 * `DStream`. To use this class you need to import `wrapper.Helper._`.
 */
object Helper {
  implicit class DStreamWrapper[T](stream: DStream[T]) {
    def toObservable = Observable[T](subscriber =>
      stream.foreachRDD(_.collect().map(subscriber.onNext))
    )
  }
}
