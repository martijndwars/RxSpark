package wrapper

import org.apache.spark.streaming.dstream.DStream
import rx.lang.scala.Observable

object Helper {

  implicit class DStreamWrapper[T](stream: DStream[T]) {
    def toObservable = Observable[T](subscriber =>
      stream.foreachRDD(_.collect().map(subscriber.onNext))
    )
  }

}
