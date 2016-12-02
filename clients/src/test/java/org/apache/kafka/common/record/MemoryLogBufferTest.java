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
package org.apache.kafka.common.record;

import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.wrapNullable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(value = Parameterized.class)
public class MemoryLogBufferTest {

    private CompressionType compression;
    private byte magic;
    private long firstOffset;
    private long pid = 134234L;
    private short epoch = 28;
    private int firstSequence = 777;

    public MemoryLogBufferTest(byte magic, long firstOffset, CompressionType compression) {
        this.magic = magic;
        this.compression = compression;
        this.firstOffset = firstOffset;
    }

    @Test
    public void testIterator() {
        MemoryLogBufferBuilder builder = MemoryLogBuffer.builder(ByteBuffer.allocate(1024), magic, compression,
                TimestampType.CREATE_TIME, firstOffset, pid, epoch, firstSequence);

        byte[][] keys = new byte[][] {"a".getBytes(), "b".getBytes(), "c".getBytes(), null, "d".getBytes(), null};
        byte[][] values = new byte[][] {"1".getBytes(), "2".getBytes(), "3".getBytes(), "4".getBytes(), null, null};
        long[] timestamps = new long[] {1, 2, 3, 4, 5, 6};

        for (int i = 0; i < keys.length; i++)
            builder.append(firstOffset + i, timestamps[i], keys[i], values[i]);

        MemoryLogBuffer logBuffer = builder.build();
        for (int iteration = 0; iteration < 2; iteration++) {
            int total = 0;
            for (LogEntry entry : logBuffer) {
                assertTrue(entry.isValid());
                assertEquals(compression, entry.compressionType());
                assertEquals(firstOffset + total, entry.firstOffset());

                if (magic >= Record.MAGIC_VALUE_V2) {
                    assertEquals(pid, entry.pid());
                    assertEquals(epoch, entry.epoch());
                    assertEquals(firstSequence + total, entry.firstSequence());
                }

                int records = 0;
                for (LogRecord record : entry) {
                    assertTrue(record.isValid());
                    assertTrue(record.hasMagic(entry.magic()));
                    assertFalse(record.isCompressed());
                    assertEquals(firstOffset + total, record.offset());
                    assertEquals(wrapNullable(keys[total]), record.key());
                    assertEquals(wrapNullable(values[total]), record.value());

                    if (magic >= Record.MAGIC_VALUE_V2)
                        assertEquals(firstSequence + total, record.sequence());

                    if (magic > Record.MAGIC_VALUE_V0) {
                        assertEquals(timestamps[total], record.timestamp());
                        if (magic < Record.MAGIC_VALUE_V2)
                            assertTrue(record.hasTimestampType(entry.timestampType()));
                    }

                    total++;
                    records++;
                }

                assertEquals(entry.firstOffset() + records - 1, entry.lastOffset());
            }
        }
    }

    @Test
    public void testHasRoomForMethod() {
        MemoryLogBufferBuilder builder = MemoryLogBuffer.builder(ByteBuffer.allocate(1024), magic, compression, TimestampType.CREATE_TIME);
        builder.append(0, 0L, "a".getBytes(), "1".getBytes());
        assertTrue(builder.hasRoomFor("b".getBytes(), "2".getBytes()));
        builder.close();
        assertFalse(builder.hasRoomFor("b".getBytes(), "2".getBytes()));
    }

    @Test
    public void testFilterTo() {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryLogBufferBuilder builder = MemoryLogBuffer.builder(buffer, magic, compression, TimestampType.CREATE_TIME);
        builder.append(0L, 10L, null, "a".getBytes());
        builder.close();

        builder = MemoryLogBuffer.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 1L);
        builder.append(1L, 11L, "1".getBytes(), "b".getBytes());
        builder.append(2L, 12L, null, "c".getBytes());
        builder.close();

        builder = MemoryLogBuffer.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 3L);
        builder.append(3L, 13L, null, "d".getBytes());
        builder.append(4L, 14L, "4".getBytes(), "e".getBytes());
        builder.append(5L, 15L, "5".getBytes(), "f".getBytes());
        builder.close();

        builder = MemoryLogBuffer.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 6L);
        builder.append(6L, 16L, "6".getBytes(), "g".getBytes());
        builder.close();

        buffer.flip();

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        MemoryLogBuffer.FilterResult result = MemoryLogBuffer.readableRecords(buffer).filterTo(new RetainNonNullKeysFilter(), filtered);

        filtered.flip();

        assertEquals(7, result.messagesRead);
        assertEquals(4, result.messagesRetained);
        assertEquals(buffer.limit(), result.bytesRead);
        assertEquals(filtered.limit(), result.bytesRetained);
        if (magic > 0) {
            assertEquals(16L, result.maxTimestamp);
            assertEquals(6L, result.offsetOfMaxTimestamp);
        }

        MemoryLogBuffer filteredRecords = MemoryLogBuffer.readableRecords(filtered);

        List<LogEntry.ShallowLogEntry> shallowEntries = TestUtils.toList(filteredRecords.shallowIterator());
        List<Long> expectedOffsets = magic < Record.MAGIC_VALUE_V2 && compression == CompressionType.NONE ? asList(1L, 4L, 5L, 6L) : asList(1L, 5L, 6L);
        assertEquals(expectedOffsets.size(), shallowEntries.size());

        for (int i = 0; i < expectedOffsets.size(); i++) {
            LogEntry shallowEntry = shallowEntries.get(i);
            assertEquals(expectedOffsets.get(i).longValue(), shallowEntry.offset());
            assertEquals(magic, shallowEntry.magic());
            assertEquals(compression, shallowEntry.compressionType());
            assertEquals(magic == Record.MAGIC_VALUE_V0 ? TimestampType.NO_TIMESTAMP_TYPE : TimestampType.CREATE_TIME,
                    shallowEntry.timestampType());
        }

        List<LogRecord> records = TestUtils.toList(filteredRecords.records());
        assertEquals(4, records.size());

        LogRecord first = records.get(0);
        assertEquals(1L, first.offset());
        if (magic > Record.MAGIC_VALUE_V0)
            assertEquals(11L, first.timestamp());
        assertEquals(ByteBuffer.wrap("1".getBytes()), first.key());
        assertEquals(ByteBuffer.wrap("b".getBytes()), first.value());

        LogRecord second = records.get(1);
        assertEquals(4L, second.offset());
        if (magic > Record.MAGIC_VALUE_V0)
            assertEquals(14L, second.timestamp());
        assertEquals(ByteBuffer.wrap("4".getBytes()), second.key());
        assertEquals(ByteBuffer.wrap("e".getBytes()), second.value());

        LogRecord third = records.get(2);
        assertEquals(5L, third.offset());
        if (magic > Record.MAGIC_VALUE_V0)
            assertEquals(15L, third.timestamp());
        assertEquals(ByteBuffer.wrap("5".getBytes()), third.key());
        assertEquals(ByteBuffer.wrap("f".getBytes()), third.value());

        LogRecord fourth = records.get(3);
        assertEquals(6L, fourth.offset());
        if (magic > Record.MAGIC_VALUE_V0)
            assertEquals(16L, fourth.timestamp());
        assertEquals(ByteBuffer.wrap("6".getBytes()), fourth.key());
        assertEquals(ByteBuffer.wrap("g".getBytes()), fourth.value());
    }

    @Test
    public void testFilterToPreservesLogAppendTime() {
        long logAppendTime = System.currentTimeMillis();

        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryLogBufferBuilder builder = MemoryLogBuffer.builder(buffer, magic, compression,
                TimestampType.LOG_APPEND_TIME, 0L, logAppendTime);
        builder.append(0L, 10L, null, "a".getBytes());
        builder.close();

        builder = MemoryLogBuffer.builder(buffer, magic, compression, TimestampType.LOG_APPEND_TIME, 1L, logAppendTime);
        builder.append(1L, 11L, "1".getBytes(), "b".getBytes());
        builder.append(2L, 12L, null, "c".getBytes());
        builder.close();

        builder = MemoryLogBuffer.builder(buffer, magic, compression, TimestampType.LOG_APPEND_TIME, 3L, logAppendTime);
        builder.append(3L, 13L, null, "d".getBytes());
        builder.append(4L, 14L, "4".getBytes(), "e".getBytes());
        builder.append(5L, 15L, "5".getBytes(), "f".getBytes());
        builder.close();

        buffer.flip();

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        MemoryLogBuffer.readableRecords(buffer).filterTo(new RetainNonNullKeysFilter(), filtered);

        filtered.flip();
        MemoryLogBuffer filteredRecords = MemoryLogBuffer.readableRecords(filtered);

        List<LogEntry.ShallowLogEntry> shallowEntries = TestUtils.toList(filteredRecords.shallowIterator());
        assertEquals(magic < Record.MAGIC_VALUE_V2 && compression == CompressionType.NONE ? 3 : 2, shallowEntries.size());

        for (LogEntry shallowEntry : shallowEntries) {
            assertEquals(compression, shallowEntry.compressionType());
            if (magic > Record.MAGIC_VALUE_V0) {
                assertEquals(TimestampType.LOG_APPEND_TIME, shallowEntry.timestampType());
                assertEquals(logAppendTime, shallowEntry.timestamp());
            }
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        for (long firstOffset : asList(0L, 57L))
            for (byte magic : asList(Record.MAGIC_VALUE_V0, Record.MAGIC_VALUE_V1, Record.MAGIC_VALUE_V2))
                for (CompressionType type: CompressionType.values())
                    values.add(new Object[] {magic, firstOffset, type});
        return values;
    }

    private static class RetainNonNullKeysFilter implements MemoryLogBuffer.LogRecordFilter {
        @Override
        public boolean shouldRetain(LogRecord record) {
            return record.hasKey();
        }
    }
}
