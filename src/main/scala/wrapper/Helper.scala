package wrapper

import java.io._
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

  implicit class ObservableWrapper[T](obs: Observable[T]) {
    def toDStream(ssc: StreamingContext) = {
      new Thread() {
        override def run(): Unit = {
          val server = new ServerSocket(9999)
          val s = server.accept()
          println("Client connected")

          val oos = new ObjectOutputStream(s.getOutputStream)
          obs.subscribe(x => oos.writeObject(x))

          while (true) {}
        }
      }.start()
      
      ssc.socketStream("localhost", 9999, (inputStream: InputStream) => {
        val objectInputStream = new ObjectInputStream(inputStream)

        new Iterator[AnyRef] {
          override def hasNext: Boolean = {
            true
          }

          override def next(): AnyRef =
            objectInputStream.readObject()
        }
      }, StorageLevel.MEMORY_AND_DISK_SER)
    }
  }

}
