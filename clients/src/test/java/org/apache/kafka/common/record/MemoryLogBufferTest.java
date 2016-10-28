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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.apache.kafka.common.utils.Utils.toArray;
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
        MemoryLogBuffer.Builder builder1 = MemoryLogBuffer.builder(ByteBuffer.allocate(1024), magic, compression, TimestampType.CREATE_TIME, firstOffset);
        MemoryLogBuffer.Builder builder2 = MemoryLogBuffer.builder(ByteBuffer.allocate(1024), magic, compression, TimestampType.CREATE_TIME, firstOffset);
        List<Record> list = Arrays.asList(Record.create(magic, 1L, "a".getBytes(), "1".getBytes()),
                                          Record.create(magic, 2L, "b".getBytes(), "2".getBytes()),
                                          Record.create(magic, 3L, "c".getBytes(), "3".getBytes()));
        for (int i = 0; i < list.size(); i++) {
            Record r = list.get(i);
            builder1.append(firstOffset + i, r);
            builder2.append(firstOffset + i, i + 1, toArray(r.key()), toArray(r.value()));
        }

        MemoryLogBuffer recs1 = builder1.build();
        MemoryLogBuffer recs2 = builder2.build();

        for (int iteration = 0; iteration < 2; iteration++) {
            for (MemoryLogBuffer recs : Arrays.asList(recs1, recs2)) {
                Iterator<LogEntry> iter = recs.iterator();
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
        MemoryLogBuffer.Builder builder = MemoryLogBuffer.builder(ByteBuffer.allocate(1024), magic, compression, TimestampType.CREATE_TIME);
        builder.append(0, Record.create(magic, 0L, "a".getBytes(), "1".getBytes()));

        assertTrue(builder.hasRoomFor("b".getBytes(), "2".getBytes()));
        builder.close();
        assertFalse(builder.hasRoomFor("b".getBytes(), "2".getBytes()));
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        for (long firstOffset : Arrays.asList(0L, 57L))
            for (byte magic : Arrays.asList(Record.MAGIC_VALUE_V0, Record.MAGIC_VALUE_V1))
                for (CompressionType type: CompressionType.values())
                    values.add(new Object[] {magic, firstOffset, type});
        return values;
    }
}
