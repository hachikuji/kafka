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

import java.nio.ByteBuffer

import kafka.common.LongRef
import kafka.message._
import org.apache.kafka.common.errors.InvalidTimestampException
import org.apache.kafka.common.record._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConverters._

class LogValidatorTest extends JUnitSuite {

  @Test
  def testLogAppendTimeNonCompressed() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val entries = createLogEntries(magicValue = Message.MagicValue_V1, timestamp = 0L, codec = CompressionType.NONE)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(entries,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = 1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedEntries = validatedResults.validatedEntries
    assertEquals("message set size should not change", entries.asScala.size, validatedEntries.asScala.size)
    validatedEntries.asScala.foreach(logEntry => validateLogAppendTime(now, logEntry.record))
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be 0", 0, validatedResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testLogAppendTimeWithRecompression() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val entries = createLogEntries(magicValue = Message.MagicValue_V0, codec = CompressionType.defaultCompressionType)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(
      entries,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = 1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedEntries = validatedResults.validatedEntries

    assertEquals("message set size should not change", entries.asScala.size, validatedEntries.asScala.size)
    validatedEntries.asScala.foreach(logEntry => validateLogAppendTime(now, logEntry.record))
    assertTrue("MessageSet should still valid", validatedEntries.iterator(true).next().record.isValid)
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${entries.asScala.size - 1}",
      entries.asScala.size - 1, validatedResults.offsetOfMaxTimestamp)
    assertTrue("Message size may have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testLogAppendTimeWithoutRecompression() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val entries = createLogEntries(magicValue = Message.MagicValue_V1,
      timestamp = 0L, codec = CompressionType.defaultCompressionType)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(
      entries,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = 1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedEntries = validatedResults.validatedEntries

    assertEquals("message set size should not change", entries.asScala.size,
      validatedEntries.asScala.size)
    validatedEntries.asScala.foreach(logEntry => validateLogAppendTime(now, logEntry.record))
    assertTrue("MessageSet should still valid", validatedEntries.iterator(true).next().record.isValid)
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${entries.asScala.size - 1}",
      entries.asScala.size - 1, validatedResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeNonCompressed() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val entries =
      MemoryLogBuffer.withRecords(CompressionType.NONE,
        Record.create(Message.MagicValue_V1, timestampSeq(0), "hello".getBytes),
        Record.create(Message.MagicValue_V1, timestampSeq(1), "there".getBytes),
        Record.create(Message.MagicValue_V1, timestampSeq(2), "beautiful".getBytes))

    val validatingResults = LogValidator.validateMessagesAndAssignOffsets(entries,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = 1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedEntries = validatingResults.validatedEntries

    var i = 0
    for (logEntry <- validatedEntries.asScala) {
      logEntry.record.ensureValid()
      assertEquals(logEntry.record.timestamp, timestampSeq(i))
      assertEquals(logEntry.record.timestampType, TimestampType.CREATE_TIME)
      i += 1
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatingResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be 1", 1, validatingResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeCompressed() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val compressedEntries =
      MemoryLogBuffer.withRecords(CompressionType.defaultCompressionType,
        Record.create(Message.MagicValue_V1, timestampSeq(0), "hello".getBytes),
        Record.create(Message.MagicValue_V1, timestampSeq(1), "there".getBytes),
        Record.create(Message.MagicValue_V1, timestampSeq(2), "beautiful".getBytes))

    val validatingCompressedEntriesResults =
      LogValidator.validateMessagesAndAssignOffsets(compressedEntries,
        offsetCounter = new LongRef(0),
        now = System.currentTimeMillis(),
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        messageFormatVersion = 1,
        messageTimestampType = TimestampType.CREATE_TIME,
        messageTimestampDiffMaxMs = 1000L)
    val validatedCompressedEntries = validatingCompressedEntriesResults.validatedEntries

    var i = 0
    for (logEntry <- validatedCompressedEntries.asScala) {
      logEntry.record.ensureValid()
      assertEquals(logEntry.record.timestamp, timestampSeq(i))
      assertEquals(logEntry.record.timestampType, TimestampType.CREATE_TIME)
      i += 1
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatingCompressedEntriesResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be ${validatedCompressedEntries.asScala.size - 1}",
      validatedCompressedEntries.asScala.size - 1, validatingCompressedEntriesResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingCompressedEntriesResults.messageSizeMaybeChanged)
  }

  @Test(expected = classOf[InvalidTimestampException])
  def testInvalidCreateTimeNonCompressed() {
    val now = System.currentTimeMillis()
    val entries = createLogEntries(magicValue = Message.MagicValue_V1, timestamp = now - 1001L,
      codec = CompressionType.NONE)
    LogValidator.validateMessagesAndAssignOffsets(
      entries,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = 1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
  }

  @Test(expected = classOf[InvalidTimestampException])
  def testInvalidCreateTimeCompressed() {
    val now = System.currentTimeMillis()
    val compressedLogBuffer = createLogEntries(magicValue = Message.MagicValue_V1, timestamp = now - 1001L,
      codec = CompressionType.defaultCompressionType)
    LogValidator.validateMessagesAndAssignOffsets(
      compressedLogBuffer,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = 1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
  }
  @Test
  def testAbsoluteOffsetAssignmentNonCompressed() {
    val entries = createLogEntries(magicValue = Message.MagicValue_V0, codec = CompressionType.NONE)
    val offset = 1234567
    checkOffsets(entries, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(entries,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = 0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedEntries, offset)
  }

  @Test
  def testAbsoluteOffsetAssignmentCompressed() {
    val compressedEntries = createLogEntries(magicValue = Message.MagicValue_V0, codec = CompressionType.defaultCompressionType)
    val offset = 1234567
    checkOffsets(compressedEntries, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(compressedEntries,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = 0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedEntries, offset)
  }

  @Test
  def testRelativeOffsetAssignmentNonCompressed() {
    val now = System.currentTimeMillis()
    val entries = createLogEntries(magicValue = Message.MagicValue_V1, timestamp = now, codec = CompressionType.NONE)
    val offset = 1234567
    checkOffsets(entries, 0)
    val messageWithOffset = LogValidator.validateMessagesAndAssignOffsets(entries,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedEntries
    checkOffsets(messageWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentCompressed() {
    val now = System.currentTimeMillis()
    val compressedEntries = createLogEntries(magicValue = Message.MagicValue_V1, timestamp = now, codec = CompressionType.defaultCompressionType)
    val offset = 1234567
    checkOffsets(compressedEntries, 0)
    val compressedMessagesWithOffset = LogValidator.validateMessagesAndAssignOffsets(
      compressedEntries,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedEntries
    checkOffsets(compressedMessagesWithOffset, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV0NonCompressed() {
    val entriesV0 = createLogEntries(magicValue = Message.MagicValue_V0, codec = CompressionType.NONE)
    checkOffsets(entriesV0, 0)
    val offset = 1234567
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(entriesV0,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = 1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedEntries, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV0Compressed() {
    val compressedEntriesV0 = createLogEntries(magicValue = Message.MagicValue_V0, codec = CompressionType.defaultCompressionType)
    val offset = 1234567
    checkOffsets(compressedEntriesV0, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(compressedEntriesV0,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = 1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedEntries, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV1NonCompressed() {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val entriesV1 = createLogEntries(Message.MagicValue_V1, now, codec = CompressionType.NONE)
    checkOffsets(entriesV1, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(entriesV1,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = 0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedEntries, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV1Compressed() {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val compressedEntriesV1 = createLogEntries(Message.MagicValue_V1, now, CompressionType.defaultCompressionType)
    checkOffsets(compressedEntriesV1, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(compressedEntriesV1,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = 0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedEntries, offset)
  }

  @Test(expected = classOf[InvalidRecordException])
  def testInvalidInnerMagicVersion(): Unit = {
    val offset = 1234567
    val entries = logBufferWithInvalidInnerMagic(offset)
    LogValidator.validateMessagesAndAssignOffsets(entries,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = SnappyCompressionCodec,
      targetCodec = SnappyCompressionCodec,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L)
  }

  private def createLogEntries(magicValue: Byte = Message.CurrentMagicValue,
                              timestamp: Long = Message.NoTimestamp,
                              codec: CompressionType = CompressionType.NONE): MemoryLogBuffer = {
    if (magicValue == Message.MagicValue_V0) {
      MemoryLogBuffer.withRecords(
        codec,
        Record.create(Record.MAGIC_VALUE_V0, Record.NO_TIMESTAMP, "hello".getBytes),
        Record.create(Record.MAGIC_VALUE_V0, Record.NO_TIMESTAMP, "there".getBytes),
        Record.create(Record.MAGIC_VALUE_V0, Record.NO_TIMESTAMP, "beautiful".getBytes))
    } else {
      MemoryLogBuffer.withRecords(
        codec,
        Record.create(Record.MAGIC_VALUE_V1, timestamp, "hello".getBytes),
        Record.create(Record.MAGIC_VALUE_V1, timestamp, "there".getBytes),
        Record.create(Record.MAGIC_VALUE_V1, timestamp, "beautiful".getBytes))
    }
  }

  /* check that offsets are assigned based on byte offset from the given base offset */
  def checkOffsets(logBuffer: MemoryLogBuffer, baseOffset: Long) {
    assertTrue("Message set should not be empty", logBuffer.asScala.nonEmpty)
    var offset = baseOffset
    for(entry <- logBuffer.asScala) {
      assertEquals("Unexpected offset in message set iterator", offset, entry.offset)
      offset += 1
    }
  }

  private def logBufferWithInvalidInnerMagic(initialOffset: Long): MemoryLogBuffer = {
    val records = (0 until 20).map(id =>
      Record.create(Record.MAGIC_VALUE_V0,
        Record.NO_TIMESTAMP,
        id.toString.getBytes,
        id.toString.getBytes))

    val buffer = ByteBuffer.allocate(math.min(math.max(records.map(_.size()).sum / 2, 1024), 1 << 16))
    val builder = MemoryLogBuffer.builder(buffer, Record.MAGIC_VALUE_V1, CompressionType.defaultCompressionType,
      TimestampType.CREATE_TIME)

    var offset = initialOffset
    records.foreach { record =>
      builder.appendUnchecked(offset, record)
      offset += 1
    }

    builder.build()
  }

  def validateLogAppendTime(now: Long, record: Record) {
    record.ensureValid()
    assertEquals(s"Timestamp of message $record should be $now", now, record.timestamp)
    assertEquals(TimestampType.LOG_APPEND_TIME, record.timestampType)
  }

}
