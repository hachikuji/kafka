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

import org.apache.kafka.common.utils.AbstractIterator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.kafka.common.record.LogBuffer.LOG_OVERHEAD;

/**
 * An offset and record pair
 */
public abstract class LogEntry implements Iterable<LogRecord> {

    public abstract long offset();

    public abstract Record record();

    public byte magic() {
        return record().magic();
    }

    public boolean isValid() {
        return record().isValid();
    }

    public void ensureValid() {
        record().ensureValid();
    }

    public long checksum() {
        return record().checksum();
    }

    public long timestamp() {
        return record().timestamp();
    }

    public TimestampType timestampType() {
        return record().timestampType();
    }

    public long firstOffset() {
        return iterator().next().offset();
    }

    public long lastOffset() {
        return offset();
    }

    public long nextOffset() {
        return offset() + 1;
    }

    public long pid() {
        return 0L;
    }

    public short epoch() {
        return 0;
    }

    public int firstSequence() {
        return 0;
    }

    public CompressionType compressionType() {
        return record().compressionType();
    }

    @Override
    public String toString() {
        return "LogEntry(" + offset() + ", " + record() + ")";
    }
    
    public int sizeInBytes() {
        return record().size() + LOG_OVERHEAD;
    }

    public boolean isCompressed() {
        return compressionType() != CompressionType.NONE;
    }

    public void writeTo(ByteBuffer buffer) {
        writeHeader(buffer, offset(), record().size());
        buffer.put(record().buffer().duplicate());
    }

    @Override
    public Iterator<LogRecord> iterator() {
        final Iterator<LogEntry> iterator = deepEntries();
        return new AbstractIterator<LogRecord>() {
            @Override
            protected LogRecord makeNext() {
                if (iterator.hasNext())
                    return new LogRecordShim(iterator.next());
                return allDone();
            }
        };
    }

    private Iterator<LogEntry> deepEntries() {
        final Iterator<LogEntry> iterator;
        if (isCompressed())
            iterator = new LogBufferIterator.DeepRecordsIterator(this, false, Integer.MAX_VALUE);
        else
            iterator = Collections.singletonList(this).iterator();
        return iterator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof LogEntry)) return false;

        LogEntry that = (LogEntry) o;

        if (offset() != that.offset()) return false;
        Record thisRecord = record();
        Record thatRecord = that.record();
        return thisRecord != null ? thisRecord.equals(thatRecord) : thatRecord == null;
    }

    @Override
    public int hashCode() {
        long offset = offset();
        Record record = record();
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + (record != null ? record.hashCode() : 0);
        return result;
    }

    public static void writeHeader(ByteBuffer buffer, long offset, int size) {
        buffer.putLong(offset);
        buffer.putInt(size);
    }

    public static void writeHeader(DataOutputStream out, long offset, int size) throws IOException {
        out.writeLong(offset);
        out.writeInt(size);
    }

    private static class SimpleLogEntry extends LogEntry {
        private final long offset;
        private final Record record;

        public SimpleLogEntry(long offset, Record record) {
            this.offset = offset;
            this.record = record;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public Record record() {
            return record;
        }

    }

    public static LogEntry create(long offset, Record record) {
        return new SimpleLogEntry(offset, record);
    }


    public static class LogRecordShim implements LogRecord {
        private final LogEntry deepEntry;

        public LogRecordShim(LogEntry deepEntry) {
            this.deepEntry = deepEntry;
        }

        public LogEntry entry() {
            return deepEntry;
        }

        @Override
        public long offset() {
            return deepEntry.offset();
        }

        @Override
        public long sequence() {
            return 0L;
        }

        @Override
        public long nextOffset() {
            return offset() + 1;
        }

        @Override
        public int sizeInBytes() {
            return deepEntry.sizeInBytes();
        }

        @Override
        public byte attributes() {
            return deepEntry.record().attributes();
        }

        @Override
        public long timestamp() {
            return deepEntry.record().timestamp();
        }

        @Override
        public long checksum() {
            return deepEntry.record().checksum();
        }

        @Override
        public boolean isValid() {
            return deepEntry.record().isValid();
        }

        @Override
        public void ensureValid() {
            deepEntry.record().ensureValid();
        }

        @Override
        public int keySize() {
            return deepEntry.record().keySize();
        }

        @Override
        public boolean hasKey() {
            return deepEntry.record().hasKey();
        }

        @Override
        public ByteBuffer key() {
            return deepEntry.record().key();
        }

        @Override
        public int valueSize() {
            return deepEntry.record().valueSize();
        }

        @Override
        public boolean hasNullValue() {
            return deepEntry.record().hasNullValue();
        }

        @Override
        public ByteBuffer value() {
            return deepEntry.record().value();
        }

        @Override
        public String toString() {
            return "LogRecordShim{" +
                    "deepEntry=" + deepEntry +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LogRecordShim that = (LogRecordShim) o;

            return deepEntry != null ? deepEntry.equals(that.deepEntry) : that.deepEntry == null;
        }

        @Override
        public int hashCode() {
            return deepEntry != null ? deepEntry.hashCode() : 0;
        }

        public boolean hasMagic(byte magic) {
            return deepEntry.magic() == magic;
        }

        @Override
        public boolean isCompressed() {
            return deepEntry.compressionType() != CompressionType.NONE;
        }

        @Override
        public boolean hasTimestampType(TimestampType timestampType) {
            return deepEntry.timestampType() == timestampType;
        }
    }

    public abstract static class ShallowLogEntry extends LogEntry {
        public abstract void setOffset(long offset);

        public abstract void setCreateTime(long timestamp);

        public abstract void setLogAppendTime(long timestamp);
    }

}
