/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.{Lock, ReentrantLock}

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import kafka.utils._
import kafka.utils.timer._
import org.apache.kafka.common.utils.KafkaThread

import scala.collection._
import scala.collection.mutable.ListBuffer

/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * @param timeoutMs Expiration timeout in milliseconds. If the operation cannot be completed before this
 *                  timeout (in other words, if [[canComplete()]] does not return true), then the operation
 *                  will be completed forcefully which will result in a call first to [[onComplete()]] and
 *                  then [[onExpiration()]]
 */
abstract class DelayedOperation(val timeoutMs: Long) {

  private val completed = new AtomicBoolean(false)

  /**
   * Check if the delayed operation is already completed
   */
  def isCompleted: Boolean = completed.get()

  /**
   * Callback to execute when a delayed operation gets expired and hence forced to complete.
   */
  def onExpiration(): Unit

  /**
   * Callback for completion. This is guaranteed to be invoked exactly once, either when
   * [[canComplete()]] returns true or when the delayed operation has expired.
   */
  def onComplete(): Unit

  /**
   * Check whether the operation is ready for immediate completion.
   */
  def canComplete(): Boolean

  /**
   * Force completing the delayed operation, if not already completed.
   * This function is triggered by the purgatory either after [[canComplete()]]
   * returns true or upon expiration.
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
   */
  final def forceComplete(completionExecutorOpt: Option[Executor]): Boolean = {
    if (completed.compareAndSet(false, true)) {
      completionExecutorOpt match {
        case None => onComplete()
        case Some(exec) => exec.execute(() => onComplete())
      }
      true
    } else {
      false
    }
  }

}

private class DelayedOperationTimerTask[T <: DelayedOperation](val operation: T, completionExecutorOpt: Option[Executor])
  extends TimerTask with Logging {

  override val delayMs: Long = operation.timeoutMs
  private val tryCompletePending = new AtomicBoolean(false)
  private val lock: Lock = new ReentrantLock

  /**
   * Try to complete the delayed operation only if the lock can be acquired without blocking.
   *
   * If threadA acquires the lock and performs the check for completion before completion criteria is met
   * and threadB satisfies the completion criteria, but fails to acquire the lock because threadA has not
   * yet released the lock, we need to ensure that completion is attempted again without blocking threadA
   * or threadB. `tryCompletePending` is set by threadB when it fails to acquire the lock and at least one
   * of threadA or threadB will attempt completion of the operation if this flag is set. This ensures that
   * every invocation of `maybeTryComplete` is followed by at least one invocation of `tryComplete` until
   * the operation is actually completed.
   */
  def tryComplete(): Boolean = {
    var retry = false
    var done = false
    do {
      if (lock.tryLock()) {
        try {
          tryCompletePending.set(false)
          done = operation.canComplete() && forceComplete()
        } finally {
          lock.unlock()
        }
        // While we were holding the lock, another thread may have invoked `maybeTryComplete` and set
        // `tryCompletePending`. In this case we should retry.
        retry = tryCompletePending.get()
      } else {
        // Another thread is holding the lock. If `tryCompletePending` is already set and this thread failed to
        // acquire the lock, then the thread that is holding the lock is guaranteed to see the flag and retry.
        // Otherwise, we should set the flag and retry on this thread since the thread holding the lock may have
        // released the lock and returned by the time the flag is set.
        retry = !tryCompletePending.getAndSet(true)
      }
    } while (!operation.isCompleted && retry)

    done
  }

  private def forceComplete(): Boolean = {
    val isCompleted = operation.forceComplete(completionExecutorOpt)
    if (isCompleted)
      cancel()
    isCompleted
  }

  def isCompleted: Boolean = operation.isCompleted

  /**
   * Defines the task that is executed on timeout
   */
  override def run(): Unit = {
    if (forceComplete())
      operation.onExpiration()
  }
}

object DelayedOperationPurgatory {

  private val Shards = 512 // Shard the watcher list to reduce lock contention

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000,
                                   reaperEnabled: Boolean = true,
                                   timerEnabled: Boolean = true,
                                   asyncCompletionEnabled: Boolean = false): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval,
      reaperEnabled, timerEnabled, asyncCompletionEnabled)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 */
class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                       timeoutTimer: Timer,
                                                       brokerId: Int = 0,
                                                       purgeInterval: Int = 1000,
                                                       reaperEnabled: Boolean = true,
                                                       timerEnabled: Boolean = true,
                                                       asyncCompletionEnabled: Boolean = false)
  extends Logging with KafkaMetricsGroup {

  /* a list of operation watching keys */
  private class WatcherList {
    val watchersByKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

    val watchersLock = new ReentrantLock()

    /*
     * Return all the current watcher lists,
     * note that the returned watchers may be removed from the list by other threads
     */
    def allWatchers = {
      watchersByKey.values
    }
  }

  private val watcherLists = Array.fill[WatcherList](DelayedOperationPurgatory.Shards)(new WatcherList)
  private def watcherList(key: Any): WatcherList = {
    watcherLists(Math.abs(key.hashCode() % watcherLists.length))
  }

  // the number of estimated total operations in the purgatory
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  // background thread expiring operations that have timed out
  private val expirationReaper = new ExpiredOperationReaper()

  // Executor to use for delayed completion (i.e. if a delayed operation cannot
  // be completed immediately on submission)
  private val delayedCompletionExecutor: Option[ExecutorService] = if (asyncCompletionEnabled) {
    Some(new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](),
      new ThreadFactory {
        override def newThread(r: Runnable): Thread = new KafkaThread(s"DelayedExecutor-$purgatoryName", r, true)
      }))
  } else {
    None
  }

  private val metricsTags = Map("delayedOperation" -> purgatoryName)

  newGauge(
    "PurgatorySize",
    new Gauge[Int] {
      def value: Int = watched
    },
    metricsTags
  )

  newGauge(
    "NumDelayedOperations",
    new Gauge[Int] {
      def value: Int = numDelayed
    },
    metricsTags
  )

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling
    // tryComplete() for each key is going to be expensive if there are many keys. Instead,
    // we do the check in the following way. Call tryComplete(). If the operation is not completed,
    // we just add the operation to all keys. Then we call tryComplete() again. At this time, if
    // the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys. This does mean that
    // if the operation is completed (by another thread) between the two tryComplete() calls, the
    // operation is unnecessarily added for watch. However, this is a less severe issue since the
    // expire reaper will clean it up periodically.

    // At this point the only thread that can attempt this operation is this current thread
    // Hence it is safe to tryComplete() without a lock
    if (operation.canComplete()) {
      return operation.forceComplete(None)
    }

    val timerTask = new DelayedOperationTimerTask(operation, delayedCompletionExecutor)
    var watchCreated = false

    for (key <- watchKeys) {
      // If the operation is already completed, stop adding it to the rest of the watcher list.
      if (timerTask.isCompleted)
        return false

      watchForOperation(key, timerTask)

      if (!watchCreated) {
        watchCreated = true
        estimatedTotalOperations.incrementAndGet()
      }
    }

    if (timerTask.tryComplete())
      return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also
    if (!timerTask.isCompleted) {
      if (timerEnabled)
        timeoutTimer.add(timerTask)

      if (timerTask.isCompleted) {
        // cancel the timer task
        timerTask.cancel()
      }
    }

    false
  }

  /**
   * Check if some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   *
   * @return the number of completed operations during this process
   */
  def checkAndComplete(key: Any): Int = {
    val wl = watcherList(key)
    val watchers = inLock(wl.watchersLock) { wl.watchersByKey.get(key) }
    val numCompleted = if (watchers == null)
      0
    else
      watchers.tryCompleteWatched()
    debug(s"Request key $key unblocked $numCompleted $purgatoryName operations")
    numCompleted
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched: Int = {
    watcherLists.foldLeft(0) { case (sum, watcherList) => sum + watcherList.allWatchers.map(_.countWatched).sum }
  }

  /**
   * Return the number of delayed operations in the expiry queue
   */
  def numDelayed: Int = timeoutTimer.size

  /**
    * Cancel watching on any delayed operations for the given key. Note the operation will not be completed
    */
  def cancelForKey(key: Any): List[T] = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watchers = wl.watchersByKey.remove(key)
      if (watchers != null)
        watchers.cancel()
      else
        Nil
    }
  }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
   */
  private def watchForOperation(key: Any, operation: DelayedOperationTimerTask[T]): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watcher = wl.watchersByKey.getAndMaybePut(key)
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (wl.watchersByKey.get(key) != watchers)
        return

      if (watchers != null && watchers.isEmpty) {
        wl.watchersByKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown(): Unit = {
    if (reaperEnabled)
      expirationReaper.shutdown()

    delayedCompletionExecutor.foreach { executor =>
      executor.shutdownNow()
      executor.awaitTermination(60, TimeUnit.SECONDS)
    }

    timeoutTimer.shutdown()
    removeMetric("PurgatorySize", metricsTags)
    removeMetric("NumDelayedOperations", metricsTags)
  }

  /**
   * A linked list of watched delayed operations based on some key
   */
  private class Watchers(val key: Any) {
    private[this] val operations = new ConcurrentLinkedQueue[DelayedOperationTimerTask[T]]()

    // count the current number of watched operations. This is O(n), so use isEmpty() if possible
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    // add the element to watch
    def watch(t: DelayedOperationTimerTask[T]): Unit = {
      operations.add(t)
    }

    // traverse the list and try to complete some watched elements
    def tryCompleteWatched(): Int = {
      var completed = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          // another thread has completed this operation, just remove it
          iter.remove()
        } else if (curr.tryComplete()) {
          iter.remove()
          completed += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      completed
    }

    def cancel(): List[T] = {
      val iter = operations.iterator()
      val cancelled = new ListBuffer[T]()
      while (iter.hasNext) {
        val curr = iter.next()
        curr.cancel()
        iter.remove()
        cancelled += curr.operation
      }
      cancelled.toList
    }

    // traverse the list and purge elements that are already completed by others
    def purgeCompleted(): Int = {
      var purged = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          iter.remove()
          purged += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long): Unit = {
    timeoutTimer.advanceClock(timeoutMs)

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    if (estimatedTotalOperations.get - numDelayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      estimatedTotalOperations.getAndSet(numDelayed)
      debug("Begin purging watch lists")
      val purged = watcherLists.foldLeft(0) {
        case (sum, watcherList) => sum + watcherList.allWatchers.map(_.purgeCompleted()).sum
      }
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d-%s".format(brokerId, purgatoryName),
    false) {

    override def doWork(): Unit = {
      advanceClock(200L)
    }
  }
}
