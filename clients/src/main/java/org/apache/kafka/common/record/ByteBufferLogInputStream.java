/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.common.record;

import org.apache.kafka.common.errors.CorruptRecordException;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.LogBuffer.LOG_OVERHEAD;

/**
 * A byte buffer backed log input stream. This class avoids the need to copy records by returning
 * slices from the underlying byte buffer.
 */
class ByteBufferLogInputStream implements LogInputStream<ByteBufferLogInputStream.ByteBufferLogEntry> {
    private final ByteBuffer buffer;
    private final int maxMessageSize;

    ByteBufferLogInputStream(ByteBuffer buffer, int maxMessageSize) {
        this.buffer = buffer;
        this.maxMessageSize = maxMessageSize;
    }

    public ByteBufferLogEntry nextEntry() throws IOException {
        int remaining = buffer.remaining();
        if (LogBuffer.LOG_OVERHEAD > remaining)
            return null;

        int size = buffer.getInt(buffer.position() + LogBuffer.SIZE_OFFSET);
        if (size < Record.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Record size is less than the minimum record overhead (%d)", Record.RECORD_OVERHEAD_V0));
        if (size > maxMessageSize)
            throw new CorruptRecordException(String.format("Record size exceeds the largest allowable message size (%d).", maxMessageSize));

        if (size + LogBuffer.LOG_OVERHEAD > remaining)
            return null;

        ByteBufferLogEntry slice = new ByteBufferLogEntry(buffer);
        buffer.position(buffer.position() + slice.size());
        return slice;
    }

    public static class ByteBufferLogEntry extends LogEntry {
        protected final ByteBuffer buffer;

        public ByteBufferLogEntry(ByteBuffer buffer) {
            ByteBuffer dup = buffer.duplicate();
            int size = dup.getInt(dup.position() + LogBuffer.SIZE_OFFSET);
            dup.limit(dup.position() + LOG_OVERHEAD + size);
            this.buffer = dup.slice();
        }

        @Override
        public long offset() {
            return buffer.getLong(0);
        }

        @Override
        public Record record() {
            ByteBuffer dup = buffer.duplicate();
            dup.position(LOG_OVERHEAD);
            return new Record(dup.slice());
        }

        public ByteBuffer buffer() {
            return buffer;
        }
    }

}
