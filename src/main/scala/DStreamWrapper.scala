import org.apache.spark.streaming.dstream.DStream
import rx.lang.scala.Observable

object DStreamWrapper {
  def toObservable[T](stream: DStream[T]) = Observable[T](subscriber =>
    stream.foreachRDD(x => {
      x.collect().map(subscriber.onNext)
    })
  )
}

// TODO: Make this an extension method on DStream, so we can do DStream.toObservable
