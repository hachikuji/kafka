/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.Records.OFFSET_OFFSET;

public abstract class OldLogEntry extends AbstractLogEntry implements LogRecord {

    public abstract Record record();

    @Override
    public long lastOffset() {
        return offset();
    }

    @Override
    public boolean isValid() {
        return record().isValid();
    }

    @Override
    public void ensureValid() {
        record().ensureValid();
    }

    @Override
    public int keySize() {
        return record().keySize();
    }

    @Override
    public boolean hasKey() {
        return record().hasKey();
    }

    @Override
    public ByteBuffer key() {
        return record().key();
    }

    @Override
    public int valueSize() {
        return record().valueSize();
    }

    @Override
    public boolean hasValue() {
        return !record().hasNullValue();
    }

    @Override
    public ByteBuffer value() {
        return record().value();
    }

    @Override
    public boolean hasMagic(byte magic) {
        return magic == record().magic();
    }

    @Override
    public boolean hasTimestampType(TimestampType timestampType) {
        return record().timestampType() == timestampType;
    }

    @Override
    public long checksum() {
        return record().checksum();
    }

    @Override
    public long maxTimestamp() {
        return timestamp();
    }

    @Override
    public long timestamp() {
        return record().timestamp();
    }

    @Override
    public TimestampType timestampType() {
        return record().timestampType();
    }

    @Override
    public long baseOffset() {
        return iterator().next().offset();
    }

    @Override
    public byte magic() {
        return record().magic();
    }

    @Override
    public CompressionType compressionType() {
        return record().compressionType();
    }

    @Override
    public int sizeInBytes() {
        return record().sizeInBytes() + LOG_OVERHEAD;
    }

    @Override
    public String toString() {
        return "OldLogEntry(" + offset() + ", " + record() + ")";
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        writeHeader(buffer, offset(), record().sizeInBytes());
        buffer.put(record().buffer().duplicate());
    }

    @Override
    public long pid() {
        return LogEntry.NO_PID;
    }

    @Override
    public short epoch() {
        return LogEntry.NO_EPOCH;
    }

    @Override
    public long sequence() {
        return LogEntry.NO_SEQUENCE;
    }

    @Override
    public int baseSequence() {
        return LogEntry.NO_SEQUENCE;
    }

    @Override
    public int lastSequence() {
        return LogEntry.NO_SEQUENCE;
    }

    @Override
    public boolean isTransactional() {
        return false;
    }

    @Override
    public int partitionLeaderEpoch() {
        return LogEntry.UNKNOWN_PARTITION_LEADER_EPOCH;
    }

    @Override
    public boolean isControlRecord() {
        return false;
    }

    /**
     * Get an iterator for the nested entries contained within this log entry. Note that
     * if the entry is not compressed, then this method will return an iterator over the
     * shallow entry only (i.e. this object).
     * @return An iterator over the records contained within this log entry
     */
    @Override
    public Iterator<LogRecord> iterator() {
        final Iterator<OldLogEntry> iterator;
        if (isCompressed())
            iterator = new DeepRecordsIterator(this, false, Integer.MAX_VALUE);
        else
            iterator = Collections.singletonList(this).iterator();

        return new AbstractIterator<LogRecord>() {
            @Override
            protected LogRecord makeNext() {
                if (iterator.hasNext())
                    return iterator.next();
                return allDone();
            }
        };
    }

    public static void writeHeader(ByteBuffer buffer, long offset, int size) {
        buffer.putLong(offset);
        buffer.putInt(size);
    }

    public static void writeHeader(DataOutputStream out, long offset, int size) throws IOException {
        out.writeLong(offset);
        out.writeInt(size);
    }

    private static final class DataLogInputStream implements LogInputStream<OldLogEntry> {
        private final DataInputStream stream;
        protected final int maxMessageSize;

        DataLogInputStream(DataInputStream stream, int maxMessageSize) {
            this.stream = stream;
            this.maxMessageSize = maxMessageSize;
        }

        public OldLogEntry nextEntry() throws IOException {
            try {
                long offset = stream.readLong();
                int size = stream.readInt();
                if (size < Record.RECORD_OVERHEAD_V0)
                    throw new CorruptRecordException(String.format("Record size is less than the minimum record overhead (%d)", Record.RECORD_OVERHEAD_V0));
                if (size > maxMessageSize)
                    throw new CorruptRecordException(String.format("Record size exceeds the largest allowable message size (%d).", maxMessageSize));

                byte[] recordBuffer = new byte[size];
                stream.readFully(recordBuffer, 0, size);
                ByteBuffer buf = ByteBuffer.wrap(recordBuffer);
                return new BasicOldLogEntry(offset, new Record(buf));
            } catch (EOFException e) {
                return null;
            }
        }
    }

    private static class DeepRecordsIterator extends AbstractIterator<OldLogEntry> {
        private final ArrayDeque<OldLogEntry> logEntries;
        private final long absoluteBaseOffset;
        private final byte wrapperMagic;

        private DeepRecordsIterator(OldLogEntry wrapperEntry, boolean ensureMatchingMagic, int maxMessageSize) {
            Record wrapperRecord = wrapperEntry.record();
            this.wrapperMagic = wrapperRecord.magic();

            CompressionType compressionType = wrapperRecord.compressionType();
            ByteBuffer wrapperValue = wrapperRecord.value();
            if (wrapperValue == null)
                throw new InvalidRecordException("Found invalid compressed record set with null value (magic = " +
                        wrapperMagic + ")");

            DataInputStream stream = new DataInputStream(compressionType.wrapForInput(
                    new ByteBufferInputStream(wrapperValue), wrapperRecord.magic()));
            LogInputStream<OldLogEntry> logStream = new DataLogInputStream(stream, maxMessageSize);

            long wrapperRecordOffset = wrapperEntry.lastOffset();
            long wrapperRecordTimestamp = wrapperRecord.timestamp();
            this.logEntries = new ArrayDeque<>();

            // If relative offset is used, we need to decompress the entire message first to compute
            // the absolute offset. For simplicity and because it's a format that is on its way out, we
            // do the same for message format version 0
            try {
                while (true) {
                    OldLogEntry logEntry = logStream.nextEntry();
                    if (logEntry == null)
                        break;

                    Record record = logEntry.record();
                    byte magic = record.magic();

                    if (ensureMatchingMagic && magic != wrapperMagic)
                        throw new InvalidRecordException("Compressed message magic " + magic +
                                " does not match wrapper magic " + wrapperMagic);

                    if (magic > LogEntry.MAGIC_VALUE_V0) {
                        Record recordWithTimestamp = new Record(
                                record.buffer(),
                                wrapperRecordTimestamp,
                                wrapperRecord.timestampType()
                        );
                        logEntry = new BasicOldLogEntry(logEntry.lastOffset(), recordWithTimestamp);
                    }
                    logEntries.addLast(logEntry);
                }

                if (logEntries.isEmpty())
                    throw new InvalidRecordException("Found invalid compressed record set with no inner records");

                if (wrapperMagic > LogEntry.MAGIC_VALUE_V0)
                    this.absoluteBaseOffset = wrapperRecordOffset - logEntries.getLast().lastOffset();
                else
                    this.absoluteBaseOffset = -1;
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                Utils.closeQuietly(stream, "records iterator stream");
            }
        }

        @Override
        protected OldLogEntry makeNext() {
            if (logEntries.isEmpty())
                return allDone();

            OldLogEntry entry = logEntries.remove();

            // Convert offset to absolute offset if needed.
            if (absoluteBaseOffset >= 0) {
                long absoluteOffset = absoluteBaseOffset + entry.lastOffset();
                entry = new BasicOldLogEntry(absoluteOffset, entry.record());
            }

            if (entry.isCompressed())
                throw new InvalidRecordException("Inner messages must not be compressed");

            return entry;
        }
    }

    private static class BasicOldLogEntry extends OldLogEntry {
        private final Record record;
        private final long offset;

        private BasicOldLogEntry(long offset, Record record) {
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BasicOldLogEntry that = (BasicOldLogEntry) o;

            if (offset != that.offset) return false;
            return record != null ? record.equals(that.record) : that.record == null;

        }

        @Override
        public int hashCode() {
            int result = record != null ? record.hashCode() : 0;
            result = 31 * result + (int) (offset ^ (offset >>> 32));
            return result;
        }
    }

    static class ByteBufferOldLogEntry extends OldLogEntry implements LogEntry.MutableLogEntry {
        private final ByteBuffer buffer;
        private final Record record;

        ByteBufferOldLogEntry(ByteBuffer buffer) {
            this.buffer = buffer;
            buffer.position(LOG_OVERHEAD);
            this.record = new Record(buffer.slice());
            buffer.position(OFFSET_OFFSET);
        }

        @Override
        public long offset() {
            return buffer.getLong(OFFSET_OFFSET);
        }

        @Override
        public Record record() {
            return record;
        }

        @Override
        public void setOffset(long offset) {
            buffer.putLong(OFFSET_OFFSET, offset);
        }

        @Override
        public void setMaxTimestamp(TimestampType timestampType, long timestamp) {
            if (record.magic() == LogEntry.MAGIC_VALUE_V0)
                throw new IllegalArgumentException("Cannot set timestamp for a record with magic = 0");

            long currentTimestamp = record.timestamp();
            // We don't need to recompute crc if the timestamp is not updated.
            if (record.timestampType() == timestampType && currentTimestamp == timestamp)
                return;

            setTimestampAndUpdateCrc(timestampType, timestamp);
        }

        @Override
        public void setPartitionLeaderEpoch(int epoch) {
            throw new UnsupportedOperationException("Magic versions prior to 2 do not support partition leader epoch");
        }

        private void setTimestampAndUpdateCrc(TimestampType timestampType, long timestamp) {
            byte attributes = record.attributes();
            buffer.put(LOG_OVERHEAD + Record.ATTRIBUTES_OFFSET, timestampType.updateAttributes(attributes));
            buffer.putLong(LOG_OVERHEAD + Record.TIMESTAMP_OFFSET, timestamp);
            long crc = record.computeChecksum();
            ByteUtils.writeUnsignedInt(buffer, LOG_OVERHEAD + Record.CRC_OFFSET, crc);
        }

        public ByteBuffer buffer() {
            return buffer;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ByteBufferOldLogEntry that = (ByteBufferOldLogEntry) o;

            return buffer != null ? buffer.equals(that.buffer) : that.buffer == null;
        }

        @Override
        public int hashCode() {
            return buffer != null ? buffer.hashCode() : 0;
        }
    }

}
