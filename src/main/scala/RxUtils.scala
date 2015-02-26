import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import rx.lang.scala.{Subscription, Observable}

import scala.reflect.ClassTag

object RxUtils {
  def createStream[T: ClassTag](scc_ : StreamingContext, observable: Observable[T]): ReceiverInputDStream[T] = {
    new RxInputDStream[T](scc_, observable, StorageLevel.MEMORY_AND_DISK_SER_2)
  }
}

class RxInputDStream[T: ClassTag](ssc_ : StreamingContext, observable: Observable[T], storageLevel: StorageLevel) extends ReceiverInputDStream[T](ssc_)  {
  override def getReceiver(): Receiver[T] = {
    new RxReceiver(observable, storageLevel)
  }
}

class RxReceiver[T](observable: Observable[T], storageLevel: StorageLevel) extends Receiver[T](storageLevel) with Logging {
  var subscription: Option[Subscription] = None

  /**
   * Subscribe to the observable when Spark signals to start.
   */
  override def onStart(): Unit = {
    subscription = Some(
      Main.clock
        .asInstanceOf[Observable[T]]
        .subscribe(x => store(x))
    )

    logInfo("Rx receiver started")
  }

  /**
   * Unsubscribe from the observable when Spark signals to stop.
   */
  override def onStop(): Unit = {
    subscription.map(_.unsubscribe())

    logInfo("Rx receiver stopped")
  }
}
