package wrapper

import org.apache.spark.Logging
import rx.lang.scala.Subscriber

import scala.collection.mutable

/**
 * Custom subscriber that requests more items only if
 *
 * @tparam T
 */
class BatchSubscriber[T] extends Subscriber[T] with Logging {
  var storage: mutable.Queue[T] = new mutable.Queue[T]
  var remaining: Int = 0
  var itemCount: Int = 1
  var halved: Boolean = false

  override def onStart(): Unit = {
    remaining = 1
    request(itemCount)
  }

  override def onNext(value: T): Unit = {
    remaining -= 1
    storage += value
    logDebug("Got " + value)
  }

  /**
   * This method halves the number of requests under high load and doubles the
   * the number of requests under low load.
   *
   * @param idle Boolean indicating the load on the system
   */
  def pulse(idle: Boolean): Unit = {
    if (remaining == 0) {
      if (idle) {
        if (halved) {
          halved = false
        } else {
          itemCount *= 2
        }
        remaining = itemCount
        request(itemCount)
        logDebug("Requested " + itemCount)
      } else {
        if (!halved) {
          itemCount /= 2
          halved = true
        }
      }
    }
  }
}
