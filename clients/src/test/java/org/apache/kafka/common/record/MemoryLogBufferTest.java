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
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.toNullableArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(value = Parameterized.class)
public class MemoryLogBufferTest {

    private CompressionType compression;
    private byte magic;
    private long firstOffset;

    public MemoryLogBufferTest(byte magic, long firstOffset, CompressionType compression) {
        this.magic = magic;
        this.compression = compression;
        this.firstOffset = firstOffset;
    }

    @Test
    public void testIterator() {
        MemoryLogBufferBuilder builder1 = MemoryLogBuffer.builder(ByteBuffer.allocate(1024), magic, compression, TimestampType.CREATE_TIME, firstOffset);
        MemoryLogBufferBuilder builder2 = MemoryLogBuffer.builder(ByteBuffer.allocate(1024), magic, compression, TimestampType.CREATE_TIME, firstOffset);
        List<Record> list = asList(
                Record.create(magic, 1L, "a".getBytes(), "1".getBytes()),
                Record.create(magic, 2L, "b".getBytes(), "2".getBytes()),
                Record.create(magic, 3L, "c".getBytes(), "3".getBytes()),
                Record.create(magic, 4L, null, "4".getBytes()),
                Record.create(magic, 5L, "e".getBytes(), null),
                Record.create(magic, 6L, null, null));

        for (int i = 0; i < list.size(); i++) {
            Record r = list.get(i);
            builder1.append(firstOffset + i, r);
            builder2.append(firstOffset + i, i + 1, toNullableArray(r.key()), toNullableArray(r.value()));
        }

        MemoryLogBuffer recs1 = builder1.build();
        MemoryLogBuffer recs2 = builder2.build();

        for (int iteration = 0; iteration < 2; iteration++) {
            for (MemoryLogBuffer recs : asList(recs1, recs2)) {
                Iterator<LogEntry> iter = recs.deepEntries();
                for (int i = 0; i < list.size(); i++) {
                    assertTrue(iter.hasNext());
                    LogEntry entry = iter.next();
                    assertEquals(firstOffset + i, entry.offset());
                    assertEquals(list.get(i), entry.record());
                    entry.record().ensureValid();
                }
                assertFalse(iter.hasNext());
            }
        }
    }

    @Test
    public void testHasRoomForMethod() {
        MemoryLogBufferBuilder builder = MemoryLogBuffer.builder(ByteBuffer.allocate(1024), magic, compression, TimestampType.CREATE_TIME);
        builder.append(0, Record.create(magic, 0L, "a".getBytes(), "1".getBytes()));

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

        buffer.flip();

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        MemoryLogBuffer.readableRecords(buffer).filterTo(new RetainNonNullKeysFilter(), filtered);

        filtered.flip();
        MemoryLogBuffer filteredRecords = MemoryLogBuffer.readableRecords(filtered);

        List<ByteBufferLogInputStream.ByteBufferLogEntry> shallowEntries = TestUtils.toList(filteredRecords.shallowEntries());
        List<Long> expectedOffsets = compression == CompressionType.NONE ? asList(1L, 4L, 5L) : asList(1L, 5L);
        assertEquals(expectedOffsets.size(), shallowEntries.size());

        for (int i = 0; i < expectedOffsets.size(); i++) {
            LogEntry shallowEntry = shallowEntries.get(i);
            assertEquals(expectedOffsets.get(i).longValue(), shallowEntry.offset());
            assertEquals(magic, shallowEntry.record().magic());
            assertEquals(compression, shallowEntry.record().compressionType());
            assertEquals(magic == Record.MAGIC_VALUE_V0 ? TimestampType.NO_TIMESTAMP_TYPE : TimestampType.CREATE_TIME,
                    shallowEntry.record().timestampType());
        }

        List<LogEntry> deepEntries = TestUtils.toList(filteredRecords.deepEntries());
        assertEquals(3, deepEntries.size());

        LogEntry first = deepEntries.get(0);
        assertEquals(1L, first.offset());
        assertEquals(Record.create(magic, 11L, "1".getBytes(), "b".getBytes()), first.record());

        LogEntry second = deepEntries.get(1);
        assertEquals(4L, second.offset());
        assertEquals(Record.create(magic, 14L, "4".getBytes(), "e".getBytes()), second.record());

        LogEntry third = deepEntries.get(2);
        assertEquals(5L, third.offset());
        assertEquals(Record.create(magic, 15L, "5".getBytes(), "f".getBytes()), third.record());
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

        List<ByteBufferLogInputStream.ByteBufferLogEntry> shallowEntries = TestUtils.toList(filteredRecords.shallowEntries());
        assertEquals(compression == CompressionType.NONE ? 3 : 2, shallowEntries.size());

        for (LogEntry shallowEntry : shallowEntries) {
            assertEquals(compression, shallowEntry.record().compressionType());
            if (magic > Record.MAGIC_VALUE_V0) {
                assertEquals(TimestampType.LOG_APPEND_TIME, shallowEntry.record().timestampType());
                assertEquals(logAppendTime, shallowEntry.record().timestamp());
            }
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        for (long firstOffset : asList(0L, 57L))
            for (byte magic : asList(Record.MAGIC_VALUE_V0, Record.MAGIC_VALUE_V1))
                for (CompressionType type: CompressionType.values())
                    values.add(new Object[] {magic, firstOffset, type});
        return values;
    }

    private static class RetainNonNullKeysFilter implements MemoryLogBuffer.LogEntryFilter {
        @Override
        public boolean shouldRetain(LogEntry entry) {
            return entry.record().hasKey();
        }
    }
}
