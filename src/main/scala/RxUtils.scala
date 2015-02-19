import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import rx.lang.scala.Observable

object RxUtils {
  def createStream(scc_ : StreamingContext, observable: Observable[Long]): ReceiverInputDStream[Long] = {
    new RxInputDStream(scc_, observable, StorageLevel.MEMORY_AND_DISK_SER_2)
  }
}

class RxInputDStream(ssc_ : StreamingContext, observable: Observable[Long], storageLevel: StorageLevel) extends ReceiverInputDStream[Long](ssc_)  {
  override def getReceiver(): Receiver[Long] = {
    new RxReceiver(observable, storageLevel)
  }
}

class RxReceiver[T](observable: Observable[T], storageLevel: StorageLevel) extends Receiver[T](storageLevel) with Logging {
  override def onStart(): Unit = {
    logInfo("Rx receiver started")
//    observable.subscribe(store(_))
    (Main.clock.asInstanceOf[Observable[T]])
      .subscribe(x => store(x))
  }

  override def onStop(): Unit = {
    logInfo("Rx receiver stopped")
  }
}
