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
  def testLogAppendTime() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val messages = createLogBuffer(magicValue = Message.MagicValue_V1, timestamp = 0L, codec = CompressionType.NONE)
    val compressedMessagesWithRecompression = createLogBuffer(magicValue = Message.MagicValue_V0, codec = CompressionType.defaultCompressionType)
    val compressedMessagesWithoutRecompression =
      createLogBuffer(magicValue = Message.MagicValue_V1, timestamp = 0L, codec = CompressionType.defaultCompressionType)

    val validatingResults = LogValidator.validateMessagesAndAssignOffsets(messages,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = 1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedMessages = validatingResults.validatedRecords

    val validatingCompressedMessagesResults =
      LogValidator.validateMessagesAndAssignOffsets(compressedMessagesWithRecompression,
        offsetCounter = new LongRef(0),
        now = now,
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        messageFormatVersion = 1,
        messageTimestampType = TimestampType.LOG_APPEND_TIME,
        messageTimestampDiffMaxMs = 1000L)
    val validatedCompressedMessages = validatingCompressedMessagesResults.validatedRecords

    val validatingCompressedMessagesWithoutRecompressionResults =
      LogValidator.validateMessagesAndAssignOffsets(compressedMessagesWithoutRecompression,
        offsetCounter = new LongRef(0),
        now = now,
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        messageFormatVersion = 1,
        messageTimestampType = TimestampType.LOG_APPEND_TIME,
        messageTimestampDiffMaxMs = 1000L)

    val validatedCompressedMessagesWithoutRecompression = validatingCompressedMessagesWithoutRecompressionResults.validatedRecords

    assertEquals("message set size should not change", messages.asScala.size, validatedMessages.asScala.size)
    validatedMessages.asScala.foreach(logEntry => validateLogAppendTime(logEntry.record))
    assertEquals(s"Max timestamp should be $now", now, validatingResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be 0", 0, validatingResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingResults.messageSizeMaybeChanged)

    assertEquals("message set size should not change", compressedMessagesWithRecompression.asScala.size, validatedCompressedMessages.asScala.size)
    validatedCompressedMessages.asScala.foreach(logEntry => validateLogAppendTime(logEntry.record))
    assertTrue("MessageSet should still valid", validatedCompressedMessages.iterator(true).next().record.isValid)
    assertEquals(s"Max timestamp should be $now", now, validatingCompressedMessagesResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${compressedMessagesWithRecompression.asScala.size - 1}",
      compressedMessagesWithRecompression.asScala.size - 1, validatingCompressedMessagesResults.offsetOfMaxTimestamp)
    assertTrue("Message size may have been changed", validatingCompressedMessagesResults.messageSizeMaybeChanged)

    assertEquals("message set size should not change", compressedMessagesWithoutRecompression.asScala.size,
      validatedCompressedMessagesWithoutRecompression.asScala.size)
    validatedCompressedMessagesWithoutRecompression.asScala.foreach(logEntry => validateLogAppendTime(logEntry.record))
    assertTrue("MessageSet should still valid", validatedCompressedMessagesWithoutRecompression.iterator(true).next().record.isValid)
    assertEquals(s"Max timestamp should be $now", now, validatingCompressedMessagesWithoutRecompressionResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${compressedMessagesWithoutRecompression.asScala.size - 1}",
      compressedMessagesWithoutRecompression.asScala.size - 1, validatingCompressedMessagesWithoutRecompressionResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingCompressedMessagesWithoutRecompressionResults.messageSizeMaybeChanged)

    def validateLogAppendTime(record: Record) {
      record.ensureValid()
      assertEquals(s"Timestamp of message $record should be $now", now, record.timestamp)
      assertEquals(TimestampType.LOG_APPEND_TIME, record.timestampType)
    }
  }

  @Test
  def testCreateTime() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val messages =
      MemoryLogBuffer.withRecords(CompressionType.NONE,
        Record.create(Message.MagicValue_V1, timestampSeq(0), "hello".getBytes),
        Record.create(Message.MagicValue_V1, timestampSeq(1), "there".getBytes),
        Record.create(Message.MagicValue_V1, timestampSeq(2), "beautiful".getBytes))
    val compressedMessages =
      MemoryLogBuffer.withRecords(CompressionType.defaultCompressionType,
        Record.create(Message.MagicValue_V1, timestampSeq(0), "hello".getBytes),
        Record.create(Message.MagicValue_V1, timestampSeq(1), "there".getBytes),
        Record.create(Message.MagicValue_V1, timestampSeq(2), "beautiful".getBytes))

    val validatingResults = LogValidator.validateMessagesAndAssignOffsets(messages,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = 1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatingResults.validatedRecords

    val validatingCompressedMessagesResults =
      LogValidator.validateMessagesAndAssignOffsets(compressedMessages,
        offsetCounter = new LongRef(0),
        now = System.currentTimeMillis(),
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        messageFormatVersion = 1,
        messageTimestampType = TimestampType.CREATE_TIME,
        messageTimestampDiffMaxMs = 1000L)
    val validatedCompressedRecords = validatingCompressedMessagesResults.validatedRecords

    var i = 0
    for (messageAndOffset <- validatedRecords.asScala) {
      messageAndOffset.record.ensureValid()
      assertEquals(messageAndOffset.record.timestamp, timestampSeq(i))
      assertEquals(messageAndOffset.record.timestampType, TimestampType.CREATE_TIME)
      i += 1
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatingResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be 1", 1, validatingResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingResults.messageSizeMaybeChanged)
    i = 0
    for (messageAndOffset <- validatedCompressedRecords.asScala) {
      messageAndOffset.record.ensureValid()
      assertEquals(messageAndOffset.record.timestamp, timestampSeq(i))
      assertEquals(messageAndOffset.record.timestampType, TimestampType.CREATE_TIME)
      i += 1
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatingResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be ${validatedCompressedRecords.asScala.size - 1}",
      validatedCompressedRecords.asScala.size - 1, validatingCompressedMessagesResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingCompressedMessagesResults.messageSizeMaybeChanged)
  }

  @Test
  def testInvalidCreateTime() {
    val now = System.currentTimeMillis()
    val logBuffer = createLogBuffer(magicValue = Message.MagicValue_V1, timestamp = now - 1001L, codec = CompressionType.NONE)
    val compressedLogBuffer = createLogBuffer(magicValue = Message.MagicValue_V1, timestamp = now - 1001L, codec = CompressionType.defaultCompressionType)

    try {
      LogValidator.validateMessagesAndAssignOffsets(logBuffer,
        offsetCounter = new LongRef(0),
        now = System.currentTimeMillis(),
        sourceCodec = NoCompressionCodec,
        targetCodec = NoCompressionCodec,
        messageFormatVersion = 1,
        messageTimestampType = TimestampType.CREATE_TIME,
        messageTimestampDiffMaxMs = 1000L)
      fail("Should throw InvalidMessageException.")
    } catch {
      case _: InvalidTimestampException =>
    }

    try {
      LogValidator.validateMessagesAndAssignOffsets(
        compressedLogBuffer,
        offsetCounter = new LongRef(0),
        now = System.currentTimeMillis(),
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        messageFormatVersion = 1,
        messageTimestampType = TimestampType.CREATE_TIME,
        messageTimestampDiffMaxMs = 1000L)
      fail("Should throw InvalidMessageException.")
    } catch {
      case _: InvalidTimestampException =>
    }
  }

  @Test
  def testAbsoluteOffsetAssignment() {
    val logBuffer = createLogBuffer(magicValue = Message.MagicValue_V0, codec = CompressionType.NONE)
    val compressedLogBuffer = createLogBuffer(magicValue = Message.MagicValue_V0, codec = CompressionType.defaultCompressionType)
    // check uncompressed offsets
    checkOffsets(logBuffer, 0)
    val offset = 1234567
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = 0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedRecords, offset)

    // check compressed messages
    checkOffsets(compressedLogBuffer, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(compressedLogBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = 0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedRecords, offset)
  }

  @Test
  def testRelativeOffsetAssignment() {
    val now = System.currentTimeMillis()
    val logBuffer = createLogBuffer(magicValue = Message.MagicValue_V1, timestamp = now, codec = CompressionType.NONE)
    val compressedLogBuffer = createLogBuffer(magicValue = Message.MagicValue_V1, timestamp = now, codec = CompressionType.defaultCompressionType)

    // check uncompressed offsets
    checkOffsets(logBuffer, 0)
    val offset = 1234567
    val messageWithOffset = LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedRecords
    checkOffsets(messageWithOffset, offset)

    // check compressed messages
    checkOffsets(compressedLogBuffer, 0)
    val compressedMessagesWithOffset = LogValidator.validateMessagesAndAssignOffsets(
      compressedLogBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedRecords
    checkOffsets(compressedMessagesWithOffset, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversion() {
    // Check up conversion
    val logBufferV0 = createLogBuffer(magicValue = Message.MagicValue_V0, codec = CompressionType.NONE)
    val compressedLogBufferV0 = createLogBuffer(magicValue = Message.MagicValue_V0, codec = CompressionType.defaultCompressionType)
    // check uncompressed offsets
    checkOffsets(logBufferV0, 0)
    val offset = 1234567
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(logBufferV0,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = 1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedRecords, offset)

    // check compressed messages
    checkOffsets(compressedLogBufferV0, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(compressedLogBufferV0,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = 1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedRecords, offset)

    // Check down conversion
    val now = System.currentTimeMillis()
    val logBufferV1 = createLogBuffer(Message.MagicValue_V1, now, codec = CompressionType.NONE)
    val compressedLogBufferV1 = createLogBuffer(Message.MagicValue_V1, now, CompressionType.defaultCompressionType)

    // check uncompressed offsets
    checkOffsets(logBufferV1, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(logBufferV1,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = 0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedRecords, offset)

    // check compressed messages
    checkOffsets(compressedLogBufferV1, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(compressedLogBufferV1,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = 0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedRecords, offset)
  }

  @Test(expected = classOf[InvalidRecordException])
  def testInvalidInnerMagicVersion(): Unit = {
    val offset = 1234567
    val messages = logBufferWithInvalidInnerMagic(offset)
    LogValidator.validateMessagesAndAssignOffsets(messages,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = SnappyCompressionCodec,
      targetCodec = SnappyCompressionCodec,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L)
    fail("Validation should have raised an error")
  }

  private def createLogBuffer(magicValue: Byte = Message.CurrentMagicValue,
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
    val builder = MemoryLogBuffer.builder(buffer, Record.MAGIC_VALUE_V1, CompressionType.defaultCompressionType, TimestampType.CREATE_TIME)

    var offset = initialOffset
    records.foreach { record =>
      builder.appendUnchecked(offset, record)
      offset += 1
    }

    builder.build()
  }

}
