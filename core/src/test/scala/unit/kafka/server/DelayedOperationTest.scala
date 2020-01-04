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

import java.util.Random
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReentrantLock

import kafka.utils.TestUtils
import org.apache.kafka.common.utils.Time
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.Assertions.intercept

import scala.collection.JavaConverters._

class DelayedOperationTest {

  var purgatory: DelayedOperationPurgatory[MockDelayedOperation] = null
  var executorService: ExecutorService = null

  @Before
  def setUp(): Unit = {
    purgatory = DelayedOperationPurgatory[MockDelayedOperation](purgatoryName = "mock")
  }

  @After
  def tearDown(): Unit = {
    purgatory.shutdown()
    if (executorService != null)
      executorService.shutdown()
  }

  @Test
  def testRequestSatisfaction(): Unit = {
    val r1 = new MockDelayedOperation(100000L)
    val r2 = new MockDelayedOperation(100000L)
    assertEquals("With no waiting requests, nothing should be satisfied", 0, purgatory.checkAndComplete("test1"))
    assertFalse("r1 not satisfied and hence watched", purgatory.tryCompleteElseWatch(r1, Array("test1")))
    assertEquals("Still nothing satisfied", 0, purgatory.checkAndComplete("test1"))
    assertFalse("r2 not satisfied and hence watched", purgatory.tryCompleteElseWatch(r2, Array("test2")))
    assertEquals("Still nothing satisfied", 0, purgatory.checkAndComplete("test2"))
    r1.completable = true
    assertEquals("r1 satisfied", 1, purgatory.checkAndComplete("test1"))
    assertEquals("Nothing satisfied", 0, purgatory.checkAndComplete("test1"))
    r2.completable = true
    assertEquals("r2 satisfied", 1, purgatory.checkAndComplete("test2"))
    assertEquals("Nothing satisfied", 0, purgatory.checkAndComplete("test2"))
  }

  @Test
  def testRequestExpiry(): Unit = {
    val expiration = 20L
    val start = Time.SYSTEM.hiResClockMs
    val r1 = new MockDelayedOperation(expiration)
    val r2 = new MockDelayedOperation(200000L)
    assertFalse("r1 not satisfied and hence watched", purgatory.tryCompleteElseWatch(r1, Array("test1")))
    assertFalse("r2 not satisfied and hence watched", purgatory.tryCompleteElseWatch(r2, Array("test2")))
    r1.awaitExpiration()
    val elapsed = Time.SYSTEM.hiResClockMs - start
    assertTrue("r1 completed due to expiration", r1.isCompleted)
    assertFalse("r2 hasn't completed", r2.isCompleted)
    assertTrue(s"Time for expiration $elapsed should at least $expiration", elapsed >= expiration)
  }

  @Test
  def testDelayedFuture(): Unit = {
    val purgatoryName = "testDelayedFuture"
    val purgatory = new DelayedFuturePurgatory(purgatoryName, brokerId = 0)
    val result = new AtomicInteger()

    def hasExecutorThread: Boolean = Thread.getAllStackTraces.keySet.asScala.map(_.getName)
      .exists(_.contains(s"DelayedExecutor-$purgatoryName"))
    def updateResult(futures: List[CompletableFuture[Integer]]): Unit =
      result.set(futures.filterNot(_.isCompletedExceptionally).map(_.get.intValue).sum)

    assertFalse("Unnecessary thread created", hasExecutorThread)

    // Two completed futures: callback should be executed immediately on the same thread
    val futures1 = List(CompletableFuture.completedFuture(10.asInstanceOf[Integer]),
      CompletableFuture.completedFuture(11.asInstanceOf[Integer]))
    val r1 = purgatory.tryCompleteElseWatch[Integer](100000L, futures1, () => updateResult(futures1))
    assertTrue("r1 not completed", r1.isCompleted)
    assertEquals(21, result.get())
    assertFalse("Unnecessary thread created", hasExecutorThread)

    // Two delayed futures: callback should wait for both to complete
    result.set(-1)
    val futures2 = List(new CompletableFuture[Integer], new CompletableFuture[Integer])
    val r2 = purgatory.tryCompleteElseWatch[Integer](100000L, futures2, () => updateResult(futures2))
    assertFalse("r2 should be incomplete", r2.isCompleted)
    futures2.head.complete(20)
    assertFalse(r2.isCompleted)
    assertEquals(-1, result.get())
    futures2(1).complete(21)
    TestUtils.waitUntilTrue(() => r2.isCompleted, "r2 not completed")
    TestUtils.waitUntilTrue(() => result.get == 41, "callback not invoked")
    assertTrue("Thread not created for executing delayed task", hasExecutorThread)

    // One immediate and one delayed future: callback should wait for delayed task to complete
    result.set(-1)
    val futures3 = List(new CompletableFuture[Integer], CompletableFuture.completedFuture(31.asInstanceOf[Integer]))
    val r3 = purgatory.tryCompleteElseWatch[Integer](100000L, futures3, () => updateResult(futures3))
    assertFalse("r3 should be incomplete", r3.isCompleted)
    assertEquals(-1, result.get())
    futures3.head.complete(30)
    TestUtils.waitUntilTrue(() => r3.isCompleted, "r3 not completed")
    TestUtils.waitUntilTrue(() => result.get == 61, "callback not invoked")


    // One future doesn't complete within timeout. Should expire and invoke callback after timeout.
    result.set(-1)
    val start = Time.SYSTEM.hiResClockMs
    val expirationMs = 2000L
    val futures4 = List(new CompletableFuture[Integer], new CompletableFuture[Integer])
    val r4 = purgatory.tryCompleteElseWatch[Integer](expirationMs, futures4, () => updateResult(futures4))
    futures4.head.complete(40)
    TestUtils.waitUntilTrue(() => futures4(1).isDone, "r4 futures not expired")
    assertTrue("r4 not completed after timeout", r4.isCompleted)
    val elapsed = Time.SYSTEM.hiResClockMs - start
    assertTrue(s"Time for expiration $elapsed should at least $expirationMs", elapsed >= expirationMs)
    assertEquals(40, futures4.head.get)
    assertEquals(classOf[org.apache.kafka.common.errors.TimeoutException],
      intercept[ExecutionException](futures4(1).get).getCause.getClass)
    assertEquals(40, result.get())
  }

  @Test
  def testRequestPurge(): Unit = {
    val r1 = new MockDelayedOperation(100000L)
    val r2 = new MockDelayedOperation(100000L)
    val r3 = new MockDelayedOperation(100000L)
    purgatory.tryCompleteElseWatch(r1, Array("test1"))
    purgatory.tryCompleteElseWatch(r2, Array("test1", "test2"))
    purgatory.tryCompleteElseWatch(r3, Array("test1", "test2", "test3"))

    assertEquals("Purgatory should have 3 total delayed operations", 3, purgatory.numDelayed)
    assertEquals("Purgatory should have 6 watched elements", 6, purgatory.watched)

    // Note watchers are cleaned up lazily as keys complete

    r2.completable = true
    purgatory.checkAndComplete("test1")
    assertTrue(r2.isCompleted)
    assertFalse(r1.isCompleted)
    assertFalse(r3.isCompleted)
    assertEquals("Purgatory should have 2 total delayed operations instead of " + purgatory.numDelayed, 2, purgatory.numDelayed)
    assertEquals("Purgatory should have 5 watched elements instead of " + purgatory.watched, 5, purgatory.watched)

    r3.completable = true
    purgatory.checkAndComplete("test2")
    assertTrue(r3.isCompleted)
    assertFalse(r1.isCompleted)
    assertEquals("Purgatory should have 1 total delayed operations instead of " + purgatory.numDelayed, 1, purgatory.numDelayed)
    assertEquals("Purgatory should have 3 watched elements instead of " + purgatory.watched, 3, purgatory.watched)

    r1.completable = true
    purgatory.checkAndComplete("test3")
    assertFalse(r1.isCompleted)
    assertEquals("Purgatory should have 1 total delayed operations instead of " + purgatory.numDelayed, 1, purgatory.numDelayed)
    assertEquals("Purgatory should have 2 watched elements instead of " + purgatory.watched, 2, purgatory.watched)

    purgatory.checkAndComplete("test1")
    assertTrue(r1.isCompleted)
    assertEquals("Purgatory should have 0 total delayed operations instead of " + purgatory.numDelayed, 0, purgatory.numDelayed)
    assertEquals("Purgatory should have 0 watched elements instead of " + purgatory.watched, 0, purgatory.watched)
  }

  @Test
  def shouldCancelForKeyReturningCancelledOperations(): Unit = {
    purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Seq("key"))
    purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Seq("key"))
    purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Seq("key2"))

    val cancelledOperations = purgatory.cancelForKey("key")
    assertEquals(2, cancelledOperations.size)
    assertEquals(1, purgatory.numDelayed)
    assertEquals(1, purgatory.watched)
  }

  @Test
  def shouldReturnNilOperationsOnCancelForKeyWhenKeyDoesntExist(): Unit = {
    val cancelledOperations = purgatory.cancelForKey("key")
    assertEquals(Nil, cancelledOperations)
  }

  /**
    * Verify that if there is lock contention between two threads attempting to complete,
    * completion is performed without any blocking in either thread.
    */
  @Test
  def testTryCompleteLockContention(): Unit = {
    executorService = Executors.newSingleThreadExecutor()
    val completionAttemptsRemaining = new AtomicInteger(Int.MaxValue)
    val tryCompleteSemaphore = new Semaphore(1)
    val key = "key"

    val op = new MockDelayedOperation(100000L) {
      override def canComplete() = {
        val shouldComplete = completionAttemptsRemaining.decrementAndGet <= 0
        tryCompleteSemaphore.acquire()
        try {
          shouldComplete
        } finally {
          tryCompleteSemaphore.release()
        }
      }
    }

    purgatory.tryCompleteElseWatch(op, Seq(key))
    completionAttemptsRemaining.set(2)
    tryCompleteSemaphore.acquire()
    val future = runOnAnotherThread(purgatory.checkAndComplete(key), shouldComplete = false)
    TestUtils.waitUntilTrue(() => tryCompleteSemaphore.hasQueuedThreads, "Not attempting to complete")
    purgatory.checkAndComplete(key) // this should not block even though lock is not free
    assertFalse("Operation should not have completed", op.isCompleted)
    tryCompleteSemaphore.release()
    future.get(10, TimeUnit.SECONDS)
    assertTrue("Operation should have completed", op.isCompleted)
  }

  /**
    * Test `tryComplete` with multiple threads to verify that there are no timing windows
    * when completion is not performed even if the thread that makes the operation completable
    * may not be able to acquire the operation lock. Since it is difficult to test all scenarios,
    * this test uses random delays with a large number of threads.
    */
  @Test
  def testTryCompleteWithMultipleThreads(): Unit = {
    val executor = Executors.newScheduledThreadPool(20)
    this.executorService = executor
    val random = new Random
    val maxDelayMs = 10
    val completionAttempts = 20

    class TestDelayOperation(index: Int) extends MockDelayedOperation(10000L) {
      val key = s"key$index"
      val completionAttemptsRemaining = new AtomicInteger(completionAttempts)

      override def canComplete(): Boolean = {
        val shouldComplete = completable
        Thread.sleep(random.nextInt(maxDelayMs))
        shouldComplete
      }
    }
    val ops = (0 until 100).map { index =>
      val op = new TestDelayOperation(index)
      purgatory.tryCompleteElseWatch(op, Seq(op.key))
      op
    }

    def scheduleTryComplete(op: TestDelayOperation, delayMs: Long): Future[_] = {
      executor.schedule(new Runnable {
        override def run(): Unit = {
          if (op.completionAttemptsRemaining.decrementAndGet() == 0)
            op.completable = true
          purgatory.checkAndComplete(op.key)
        }
      }, delayMs, TimeUnit.MILLISECONDS)
    }

    (1 to completionAttempts).flatMap { _ =>
      ops.map { op => scheduleTryComplete(op, random.nextInt(maxDelayMs)) }
    }.foreach { future => future.get }

    ops.foreach { op => assertTrue("Operation should have completed", op.isCompleted) }
  }

  @Test
  def testDelayedOperationLockHeldInCanComplete(): Unit = {
    class BlockingOperation(timeoutMs: Long, val responseLockOpt: Option[ReentrantLock] = None)
      extends MockDelayedOperation(timeoutMs){

      val latch: CountDownLatch = new CountDownLatch(1)
      val isAwaiting: AtomicBoolean = new AtomicBoolean(false)

      override def canComplete(): Boolean =  {
        if (completable) {
          isAwaiting.set(true)
          latch.await()
          true
        } else {
          false
        }
      }
    }

    val key = "key"
    val op = new BlockingOperation(100000L)

    executorService = Executors.newSingleThreadExecutor

    purgatory.tryCompleteElseWatch(op, Seq(key))
    op.completable = true

    executorService.submit(() => purgatory.checkAndComplete(key))
    TestUtils.waitUntilTrue(() => op.isAwaiting.get, "Failed to begin wait on latch", pause = 10)

    purgatory.checkAndComplete(key)
    assertFalse(op.isCompleted)

    op.latch.countDown()
    TestUtils.waitUntilTrue(() => op.isCompleted, "Operation failed to complete as expected", pause = 10)
  }

  @Test
  def testAsyncCompletion(): Unit = {
    purgatory = DelayedOperationPurgatory[MockDelayedOperation](
      purgatoryName = "mock",
      asyncCompletionEnabled = true)

    class BlockingOperation(timeoutMs: Long, val responseLockOpt: Option[ReentrantLock] = None)
      extends MockDelayedOperation(timeoutMs){

      val latch: CountDownLatch = new CountDownLatch(1)
      val finishedCompletion: AtomicBoolean = new AtomicBoolean(false)

      override def onComplete(): Unit =  {
        latch.await()
        finishedCompletion.set(true)
      }
    }

    val key = "key"
    val op = new BlockingOperation(100000L)

    purgatory.tryCompleteElseWatch(op, Seq(key))
    op.completable = true

    purgatory.checkAndComplete(key)
    assertTrue(op.isCompleted)
    assertFalse(op.finishedCompletion.get)

    op.latch.countDown()
    TestUtils.waitUntilTrue(() => op.finishedCompletion.get, "Operation failed to complete as expected", pause = 10)
  }

  private def runOnAnotherThread(fun: => Unit, shouldComplete: Boolean): Future[_] = {
    val future = executorService.submit(new Runnable {
      def run() = fun
    })
    if (shouldComplete)
      future.get()
    else
      assertFalse("Should not have completed", future.isDone)
    future
  }

  class MockDelayedOperation(timeoutMs: Long) extends DelayedOperation(timeoutMs) {
    var completable = false

    def awaitExpiration(): Unit = {
      synchronized {
        wait()
      }
    }

    override def canComplete(): Boolean = {
      completable
    }

    override def onExpiration(): Unit = {}

    override def onComplete(): Unit = {
      synchronized {
        notify()
      }
    }
  }

}
