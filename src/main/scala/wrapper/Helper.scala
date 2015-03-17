package wrapper

import java.io.PrintStream
import java.net.ServerSocket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import rx.lang.scala.Observable

object Helper {

  implicit class DStreamWrapper[T](stream: DStream[T]) {
    def toObservable = Observable[T](subscriber =>
      stream.foreachRDD(_.collect().map(subscriber.onNext))
    )
  }

  implicit class ObservableWrapper(obs: Observable[Long]) {
    def toDStream(ssc: StreamingContext) = {
      new Thread() {
        override def run: Unit = {

          val server = new ServerSocket(9999)
          val s = server.accept()
          println("Client connected")
          val out = new PrintStream(s.getOutputStream)

          obs.subscribe(x => out.println(x))
          while (true) {}
        }
      }.start


      ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    }
  }

}
