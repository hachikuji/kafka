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

package kafka.log

import java.io.File
import java.util.Properties

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{DuplicateSequenceNumberException, OutOfOrderSequenceException, ProducerFencedException}
import org.apache.kafka.common.record.ControlRecordType
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

class ProducerIdMappingTest extends JUnitSuite {
  var idMappingDir: File = null
  var config: LogConfig = null
  var idMapping: ProducerIdMapping = null
  val partition = new TopicPartition("test", 0)
  val pid = 1L
  val maxPidExpirationMs = 60 * 1000
  val time = new MockTime

  @Before
  def setUp(): Unit = {
    // Create configuration including number of snapshots to hold
    val props = new Properties()
    props.setProperty(LogConfig.MaxIdMapSnapshotsProp, "1")
    config = LogConfig(props)

    // Create temporary directory
    idMappingDir = TestUtils.tempDir()

    // Instantiate IdMapping
    idMapping = new ProducerIdMapping(config, partition, idMappingDir, maxPidExpirationMs)
  }

  @After
  def tearDown(): Unit = {
    idMappingDir.listFiles().foreach(f => f.delete())
    idMappingDir.deleteOnExit()
  }

  @Test
  def testBasicIdMapping(): Unit = {
    val epoch = 0.toShort

    // First entry for id 0 added
    append(idMapping, pid, 0, epoch, 0L, 0L)

    // Second entry for id 0 added
    append(idMapping, pid, 1, epoch, 0L, 1L)

    // Duplicate sequence number (matches previous sequence number)
    assertThrows[DuplicateSequenceNumberException] {
      append(idMapping, pid, 1, epoch, 0L, 1L)
    }

    // Invalid sequence number (greater than next expected sequence number)
    assertThrows[OutOfOrderSequenceException] {
      append(idMapping, pid, 5, epoch, 0L, 2L)
    }

    // Change epoch
    append(idMapping, pid, 0, (epoch + 1).toShort, 0L, 3L)

    // Incorrect epoch
    assertThrows[ProducerFencedException] {
      append(idMapping, pid, 0, epoch, 0L, 4L)
    }
  }

  @Test
  def testTakeSnapshot(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L, 0L)
    append(idMapping, pid, 1, epoch, 1L, 1L)

    // Take snapshot
    idMapping.maybeTakeSnapshot()

    // Check that file exists and it is not empty
    assertEquals("Directory doesn't contain a single file as expected", 1, idMappingDir.list().length)
    assertTrue("Snapshot file is empty", idMappingDir.list().head.length > 0)
  }

  @Test
  def testRecoverFromSnapshot(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L, 1L)
    append(idMapping, pid, 1, epoch, 1L, 2L)

    idMapping.maybeTakeSnapshot()
    val recoveredMapping = new ProducerIdMapping(config, partition, idMappingDir, maxPidExpirationMs)
    recoveredMapping.truncateAndReload(1L)

    // entry added after recovery
    append(recoveredMapping, pid, 2, epoch, 2L, 3L)
  }

  @Test
  def testRemoveOldSnapshot(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L, 1L)
    append(idMapping, pid, 1, epoch, 1L, 2L)

    idMapping.maybeTakeSnapshot()

    append(idMapping, pid, 2, epoch, 2L, 3L)

    idMapping.maybeTakeSnapshot()

    assertEquals(s"number of snapshot files is incorrect: ${idMappingDir.listFiles().length}",
               1, idMappingDir.listFiles().length)
  }

  @Test
  def testSkipSnapshotIfOffsetUnchanged(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L, 0L)

    idMapping.maybeTakeSnapshot()

    // nothing changed so there should be no new snapshot
    idMapping.maybeTakeSnapshot()

    assertEquals(s"number of snapshot files is incorrect: ${idMappingDir.listFiles().length}",
      1, idMappingDir.listFiles().length)
  }

  @Test
  def testStartOffset(): Unit = {
    val epoch = 0.toShort
    val pid2 = 2L
    append(idMapping, pid2, 0, epoch, 0L, 1L)
    append(idMapping, pid, 0, epoch, 1L, 2L)
    append(idMapping, pid, 1, epoch, 2L, 3L)
    append(idMapping, pid, 2, epoch, 3L, 4L)
    idMapping.maybeTakeSnapshot()

    intercept[OutOfOrderSequenceException] {
      val recoveredMapping = new ProducerIdMapping(config, partition, idMappingDir, maxPidExpirationMs)
      recoveredMapping.truncateAndReload(1L)
      append(recoveredMapping, pid2, 1, epoch, 4L, 5L)
    }
  }

  @Test(expected = classOf[OutOfOrderSequenceException])
  def testPidExpirationTimeout() {
    val epoch = 5.toShort
    val sequence = 37
    append(idMapping, pid, sequence, epoch, 1L)
    time.sleep(maxPidExpirationMs + 1)
    idMapping.checkForExpiredPids(time.milliseconds)
    append(idMapping, pid, sequence + 1, epoch, 1L)
  }

  @Test
  def testFirstUnstableOffset() {
    val epoch = 5.toShort
    val sequence = 0

    assertEquals(None, idMapping.firstUnstableOffset)

    append(idMapping, pid, sequence, epoch, offset = 99, isTransactional = true)
    assertEquals(Some(99L), idMapping.firstUnstableOffset)

    val anotherPid = 2L
    append(idMapping, anotherPid, sequence, epoch, offset = 105, isTransactional = true)
    assertEquals(Some(99L), idMapping.firstUnstableOffset)

    appendControl(idMapping, pid, epoch, ControlRecordType.COMMIT, offset = 109)
    assertEquals(Some(105L), idMapping.firstUnstableOffset)

    appendControl(idMapping, anotherPid, epoch, ControlRecordType.ABORT, offset = 112)
    assertEquals(None, idMapping.firstUnstableOffset)
  }

  @Test
  def testProducersWithOngoingTransactionsDontExpire() {
    val epoch = 5.toShort
    val sequence = 0

    append(idMapping, pid, sequence, epoch, offset = 99, isTransactional = true)
    assertEquals(Some(99L), idMapping.firstUnstableOffset)

    time.sleep(maxPidExpirationMs + 1)
    idMapping.checkForExpiredPids(time.milliseconds)

    assertTrue(idMapping.lastEntry(pid).isDefined)
    assertEquals(Some(99L), idMapping.firstUnstableOffset)

    idMapping.checkForExpiredPids(time.milliseconds)
    assertTrue(idMapping.lastEntry(pid).isDefined)
  }

  @Test(expected = classOf[ProducerFencedException])
  def testOldEpochForControlRecord(): Unit = {
    val epoch = 5.toShort
    val sequence = 0

    assertEquals(None, idMapping.firstUnstableOffset)

    append(idMapping, pid, sequence, epoch, offset = 99, isTransactional = true)
    appendControl(idMapping, pid, 3.toShort, ControlRecordType.COMMIT, offset=100)
  }

  private def appendControl(mapping: ProducerIdMapping,
                            pid: Long,
                            epoch: Short,
                            controlType: ControlRecordType,
                            offset: Long,
                            timestamp: Long = time.milliseconds()): Unit = {
    val producerAppendInfo = new ProducerAppendInfo(pid, mapping.lastEntry(pid).getOrElse(ProducerIdEntry.Empty))
    producerAppendInfo.appendControl(ControlRecord(controlType, pid, epoch, offset, timestamp))
    mapping.update(producerAppendInfo)
  }

  private def append(mapping: ProducerIdMapping,
                             pid: Long,
                             seq: Int,
                             epoch: Short,
                             offset: Long,
                             timestamp: Long = time.milliseconds(),
                             isTransactional: Boolean = false): Unit = {
    val producerAppendInfo = new ProducerAppendInfo(pid, mapping.lastEntry(pid).getOrElse(ProducerIdEntry.Empty))
    producerAppendInfo.append(epoch, seq, seq, timestamp, offset, isTransactional)
    mapping.update(producerAppendInfo)
  }

}
