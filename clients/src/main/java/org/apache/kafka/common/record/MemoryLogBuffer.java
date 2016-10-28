/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.record;

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link LogBuffer} implementation backed by a ByteBuffer.
 */
public class MemoryLogBuffer extends AbstractLogBuffer {

    public final static MemoryLogBuffer EMPTY = MemoryLogBuffer.readableRecords(ByteBuffer.allocate(0));

    // the underlying buffer used for read; while the records are still writable it is null
    private ByteBuffer buffer;

    // Construct a writable memory records
    private MemoryLogBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int sizeInBytes() {
        return buffer.limit();
    }

    @Override
    public long writeTo(GatheringByteChannel channel, long position, int length) throws IOException {
        ByteBuffer dup = buffer.duplicate();
        dup.position(new Long(position).intValue());
        return channel.write(dup);
    }

    /**
     * Write the full contents of this log buffer to the given channel (including partial records).
     * @param channel The channel to write to
     * @return The number of bytes written
     * @throws IOException For any IO errors writing to the channel
     */
    public int writeFullyTo(GatheringByteChannel channel) throws IOException {
        buffer.mark();
        int written = 0;
        while (written < sizeInBytes())
            written += channel.write(buffer);
        buffer.reset();
        return written;
    }

    /**
     * The total number of bytes in this message set not including any partial, trailing messages. This
     * may be smaller than what is returned by {@link #sizeInBytes()}.
     * @return The number of valid bytes
     */
    public int validBytes() {
        // TODO: old version cached the computed value. is it worth it?
        int bytes = 0;
        Iterator<LogEntry> iterator = iterator(true);
        while (iterator.hasNext())
            bytes += iterator.next().size();
        return bytes;
    }

    /**
     * Filter this log buffer into the provided ByteBuffer.
     * @param filter The filter function
     * @param buffer The byte buffer to write the filtered records to
     * @return A FilterResult with a summary of the output (for metrics)
     */
    public FilterResult filterTo(LogEntryFilter filter, ByteBuffer buffer) {
        long maxTimestamp = Record.NO_TIMESTAMP;
        long offsetOfMaxTimestamp = -1L;
        int messagesRead = 0;
        int bytesRead = 0;
        int messagesRetained = 0;
        int bytesRetained = 0;

        Iterator<LogEntry> shallowIterator = this.iterator(true);
        while (shallowIterator.hasNext()) {
            LogEntry shallowEntry = shallowIterator.next();
            messagesRead += 1;
            bytesRead += shallowEntry.size();

            // We use the absolute offset to decide whether to retain the message or not (this is handled by the
            // deep iterator). Because of KAFKA-4298, we have to allow for the possibility that a previous version
            // corrupted the log by writing a compressed message set with a wrapper magic value not matching the magic
            // of the inner messages. This will be fixed as we recopy the messages to the destination buffer.

            Record shallowRecord = shallowEntry.record();
            byte shallowMagic = shallowRecord.magic();
            boolean writeOriginalEntry = true;
            List<LogEntry> retainedEntries = new ArrayList<>();

            for (LogEntry deepEntry : shallowEntry) {
                Record deepRecord = deepEntry.record();
                messagesRead += 1;

                if (filter.shouldRetain(deepEntry)) {
                    // Check for log corruption due to KAFKA-4298. If we find it, make sure that we overwrite
                    // the corrupted entry with correct data.
                    if (shallowMagic != deepRecord.magic())
                        writeOriginalEntry = false;

                    retainedEntries.add(deepEntry);
                    // We need the max timestamp and last offset for time index
                    if (deepRecord.timestamp() > maxTimestamp)
                        maxTimestamp = deepRecord.timestamp();
                } else {
                    writeOriginalEntry = false;
                }
            }

            // TODO: Is this correct? There's no guarantee that timestamps increase monotonically
            if (!retainedEntries.isEmpty())
                offsetOfMaxTimestamp = retainedEntries.get(retainedEntries.size() - 1).offset();

            if (writeOriginalEntry) {
                // There are no messages compacted out and no message format conversion, write the original message set back
                shallowEntry.writeTo(buffer);
                messagesRetained += 1;
                bytesRetained += shallowRecord.size();
            } else if (!retainedEntries.isEmpty()) {
                ByteBuffer slice = buffer.slice();
                Builder builder = builderWithEntries(slice, shallowRecord.timestampType(), shallowRecord.compressionType(),
                        System.currentTimeMillis(), retainedEntries);
                builder.close();
                buffer.position(buffer.position() + slice.limit());
                messagesRetained += 1;
                bytesRetained += slice.limit();
            }
        }

        return new FilterResult(messagesRead, bytesRead, messagesRetained, bytesRetained, maxTimestamp, offsetOfMaxTimestamp);
    }

    /**
     * Get the byte buffer that backs this instance for reading.
     */
    public ByteBuffer buffer() {
        return buffer.duplicate();
    }

    @Override
    public Iterator<LogEntry> iterator() {
        return iterator(false, false, Integer.MAX_VALUE);
    }

    @Override
    public Iterator<LogEntry> iterator(boolean isShallow) {
        return iterator(isShallow, false, Integer.MAX_VALUE);
    }

    public Iterator<LogEntry> iterator(boolean isShallow, boolean ensureMatchingMagic) {
        return iterator(isShallow, ensureMatchingMagic, Integer.MAX_VALUE);
    }

    public Iterator<LogEntry> iterator(boolean isShallow, boolean ensureMatchingMagic, int maxMessageSize) {
        return new LogBufferIterator(new ByteBufferLogInputStream(buffer.duplicate(), maxMessageSize), isShallow,
                ensureMatchingMagic, maxMessageSize);
    }
    
    @Override
    public String toString() {
        Iterator<LogEntry> iter = iterator();
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        while (iter.hasNext()) {
            LogEntry entry = iter.next();
            builder.append('(');
            builder.append("offset=");
            builder.append(entry.offset());
            builder.append(",");
            builder.append("record=");
            builder.append(entry.record());
            builder.append(")");
        }
        builder.append(']');
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemoryLogBuffer that = (MemoryLogBuffer) o;

        return buffer.equals(that.buffer);
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    public interface LogEntryFilter {
        boolean shouldRetain(LogEntry entry);
    }

    public static class FilterResult {
        public final int messagesRead;
        public final int bytesRead;
        public final int messagesRetained;
        public final int bytesRetained;
        public final long maxTimestamp;
        public final long offsetOfMaxTimestamp;

        public FilterResult(int messagesRead,
                            int bytesRead,
                            int messagesRetained,
                            int bytesRetained,
                            long maxTimestamp,
                            long offsetOfMaxTimestamp) {
            this.messagesRead = messagesRead;
            this.bytesRead = bytesRead;
            this.messagesRetained = messagesRetained;
            this.bytesRetained = bytesRetained;
            this.maxTimestamp = maxTimestamp;
            this.offsetOfMaxTimestamp = offsetOfMaxTimestamp;
        }
    }

    public static class RecordsInfo {
        public final long maxTimestamp;
        public final long offsetOfMaxTimestamp;

        public RecordsInfo(long maxTimestamp,
                           long offsetOfMaxTimestamp) {
            this.maxTimestamp = maxTimestamp;
            this.offsetOfMaxTimestamp = offsetOfMaxTimestamp;
        }
    }

    public static Builder builder(ByteBuffer buffer,
                                  CompressionType compressionType,
                                  TimestampType timestampType,
                                  int writeLimit) {
        return new Builder(buffer, Record.CURRENT_MAGIC_VALUE, compressionType, timestampType, 0L, System.currentTimeMillis(), writeLimit);
    }

    public static Builder builder(ByteBuffer buffer,
                                  byte magic,
                                  CompressionType compressionType,
                                  TimestampType timestampType,
                                  long baseOffset,
                                  long logAppendTime) {
        return new Builder(buffer, magic, compressionType, timestampType, baseOffset, logAppendTime, buffer.capacity());
    }

    public static Builder builder(ByteBuffer buffer,
                                  CompressionType compressionType,
                                  TimestampType timestampType) {
        // use the buffer capacity as the default write limit
        return builder(buffer, compressionType, timestampType, buffer.capacity());
    }

    public static Builder builder(ByteBuffer buffer,
                                  byte magic,
                                  CompressionType compressionType,
                                  TimestampType timestampType) {
        return builder(buffer, magic, compressionType, timestampType, 0L);
    }

    public static Builder builder(ByteBuffer buffer,
                                  byte magic,
                                  CompressionType compressionType,
                                  TimestampType timestampType,
                                  long baseOffset) {
        return builder(buffer, magic, compressionType, timestampType, baseOffset, System.currentTimeMillis());
    }

    public static MemoryLogBuffer readableRecords(ByteBuffer buffer) {
        return new MemoryLogBuffer(buffer);
    }

    public static LogUpdater inPlaceUpdater(ByteBuffer buffer) {
        return new LogUpdater(buffer.duplicate());
    }

    public static MemoryLogBuffer withLogEntries(CompressionType compressionType, List<LogEntry> entries) {
        return withLogEntries(TimestampType.CREATE_TIME, compressionType, System.currentTimeMillis(), entries);
    }

    public static MemoryLogBuffer withLogEntries(LogEntry ... entries) {
        return withLogEntries(CompressionType.NONE, Arrays.asList(entries));
    }

    public static MemoryLogBuffer withRecords(CompressionType compressionType, long initialOffset, List<Record> records) {
        return withLogEntries(compressionType, buildLogEntries(initialOffset, records));
    }

    public static MemoryLogBuffer withRecords(Record ... records) {
        return withRecords(CompressionType.NONE, 0L, Arrays.asList(records));
    }

    public static MemoryLogBuffer withRecords(long initialOffset, Record ... records) {
        return withRecords(CompressionType.NONE, initialOffset, Arrays.asList(records));
    }

    public static MemoryLogBuffer withRecords(CompressionType compressionType, Record ... records) {
        return withRecords(compressionType, 0L, Arrays.asList(records));
    }

    private static MemoryLogBuffer withLogEntries(TimestampType timestampType,
                                                  CompressionType compressionType,
                                                  long logAppendTime,
                                                  List<LogEntry> entries) {
        if (entries.isEmpty())
            return MemoryLogBuffer.EMPTY;
        return builderWithEntries(timestampType, compressionType, logAppendTime, entries).build();
    }

    private static List<LogEntry> buildLogEntries(long initialOffset, List<Record> records) {
        List<LogEntry> entries = new ArrayList<>();
        for (Record record : records)
            entries.add(LogEntry.create(initialOffset++, record));
        return entries;
    }

    public static Builder builderWithEntries(TimestampType timestampType,
                                             CompressionType compressionType,
                                             long logAppendTime,
                                             List<LogEntry> entries) {
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize(compressionType, entries));
        return builderWithEntries(buffer, timestampType, compressionType, logAppendTime, entries);
    }

    private static Builder builderWithEntries(ByteBuffer buffer,
                                              TimestampType timestampType,
                                              CompressionType compressionType,
                                              long logAppendTime,
                                              List<LogEntry> entries) {
        if (entries.isEmpty())
            throw new IllegalArgumentException();

        LogEntry firstEntry = entries.iterator().next();
        long firstOffset = firstEntry.offset();
        byte magic = firstEntry.record().magic();

        Builder builder = MemoryLogBuffer.builder(buffer, magic, compressionType, timestampType,
                firstOffset, logAppendTime);
        for (LogEntry entry : entries)
            builder.append(entry);

        return builder;
    }

    public static class MutableLogEntry extends ByteBufferLogInputStream.ByteBufferLogEntry {

        public MutableLogEntry(ByteBuffer buffer) {
            super(buffer);
        }

        public void setOffset(long offset) {
            buffer.putLong(LogBuffer.OFFSET_OFFSET, offset);
        }

        public void setCreateTime(long timestamp) {
            Record record = record();
            if (record.magic() > 0) {
                long currentTimestamp = record.timestamp();
                // We don't need to recompute crc if the timestamp is not updated.
                if (record.timestampType() == TimestampType.CREATE_TIME && currentTimestamp == timestamp)
                    return;

                byte attributes = record.attributes();
                buffer.put(LOG_OVERHEAD + Record.ATTRIBUTES_OFFSET, TimestampType.CREATE_TIME.updateAttributes(attributes));
                buffer.putLong(LOG_OVERHEAD + Record.TIMESTAMP_OFFSET, timestamp);

                long crc = Record.computeChecksum(buffer,
                        LOG_OVERHEAD + Record.MAGIC_OFFSET,
                        buffer.limit() - Record.MAGIC_OFFSET - LOG_OVERHEAD);
                Utils.writeUnsignedInt(buffer, LOG_OVERHEAD + Record.CRC_OFFSET, crc);
            }
        }

        public void setLogAppendTime(long timestamp) {
            Record record = record();
            if (record.magic() > 0) {
                byte attributes = record.attributes();
                buffer.put(LOG_OVERHEAD + Record.ATTRIBUTES_OFFSET, TimestampType.LOG_APPEND_TIME.updateAttributes(attributes));
                buffer.putLong(LOG_OVERHEAD + Record.TIMESTAMP_OFFSET, timestamp);

                long crc = Record.computeChecksum(buffer,
                        LOG_OVERHEAD + Record.MAGIC_OFFSET,
                        buffer.limit() - Record.MAGIC_OFFSET - LOG_OVERHEAD);
                Utils.writeUnsignedInt(buffer, LOG_OVERHEAD + Record.CRC_OFFSET, crc);
            }
        }
    }

    public static class LogUpdater extends AbstractIterator<MutableLogEntry> {
        private MutableLogEntry current;
        private Iterator<ByteBufferLogInputStream.ByteBufferLogEntry> iterator;
        private long maxTimestamp = Record.NO_TIMESTAMP;
        private long offsetOfMaxTimestamp = -1;

        public LogUpdater(ByteBuffer buffer) {
            this.iterator = LogBufferIterator.shallowIterator(new ByteBufferLogInputStream(buffer, Integer.MAX_VALUE));
        }

        private void maybeUpdateMaxTimestamp() {
            if (current != null) {
                long timestamp = current.record().timestamp();
                if (timestamp > maxTimestamp) {
                    maxTimestamp = timestamp;
                    offsetOfMaxTimestamp = current.offset();
                }
            }
        }

        @Override
        protected MutableLogEntry makeNext() {
            maybeUpdateMaxTimestamp();

            if (!iterator.hasNext())
                return allDone();

            current = new MutableLogEntry(iterator.next().buffer());
            return current;
        }

        public RecordsInfo build() {
            maybeUpdateMaxTimestamp();
            return new RecordsInfo(maxTimestamp, offsetOfMaxTimestamp);
        }
    }

    public static class Builder {
        private final Compressor compressor;
        private final int writeLimit;
        private final ByteBuffer buffer;
        private final int initialCapacity;
        private final byte magic;
        private MemoryLogBuffer records;

        private Builder(ByteBuffer buffer,
                        byte magic,
                        CompressionType compressionType,
                        TimestampType timestampType,
                        long baseOffset,
                        long logAppendTime,
                        int writeLimit) {
            this.buffer = buffer;
            this.initialCapacity = buffer.capacity();
            this.writeLimit = writeLimit;
            this.magic = magic;
            this.compressor = new Compressor(buffer, magic, compressionType, timestampType, baseOffset, logAppendTime);
        }

        public MemoryLogBuffer build() {
            close();
            return records();
        }

        public ByteBuffer buffer() {
            return buffer;
        }

        public void close() {
            if (records == null) {
                compressor.close();
                ByteBuffer buffer = compressor.buffer();
                buffer.flip();
                records = MemoryLogBuffer.readableRecords(buffer);
            }
        }

        public MemoryLogBuffer records() {
            if (records != null)
                return records;
            ByteBuffer buffer = this.buffer.duplicate();
            buffer.flip();
            return MemoryLogBuffer.readableRecords(buffer);
        }

        public RecordsInfo info() {
            return new RecordsInfo(compressor.maxTimestamp(), compressor.offsetOfMaxTimestamp());
        }

        /**
         * Append the given record and offset to the buffer
         * @param offset The absolute offset of the record in the log buffer
         * @param record The record to append
         */
        public void append(long offset, Record record) {
            compressor.putRecord(offset, record);
        }

        /**
         * Append a record without doing offset/magic validation (used in testing).
         * @param offset The absolute offset of the record in the log buffer
         * @param record The record to append
         */
        public void appendUnchecked(long offset, Record record) {
            compressor.putRecordUnchecked(offset, record);
        }

        /**
         * Append the given record and offset to the buffer, converting to the write
         * message format if necessary.
         */
        public void convertAndAppend(long offset, Record record) {
            compressor.convertAndPutRecord(offset, record);
        }

        /**
         * Append the given log entry
         * @param entry The entry to append
         */
        public void append(LogEntry entry) {
            compressor.putRecord(entry.offset(), entry.record());
        }

        /**
         * Append a new record and offset to the buffer
         * @param offset The absolute offset of the record in the log buffer
         * @param timestamp The record timestamp
         * @param key The record key
         * @param value The record value
         * @return crc of the record
         */
        public long append(long offset, long timestamp, byte[] key, byte[] value) {
            return compressor.putRecord(offset, timestamp, key, value);
        }

        /**
         * Check if we have room for a new record containing the given key/value pair
         *
         * Note that the return value is based on the estimate of the bytes written to the compressor, which may not be
         * accurate if compression is really used. When this happens, the following append may cause dynamic buffer
         * re-allocation in the underlying byte buffer stream.
         *
         * There is an exceptional case when appending a single message whose size is larger than the batch size, the
         * capacity will be the message size which is larger than the write limit, i.e. the batch size. In this case
         * the checking should be based on the capacity of the initialized buffer rather than the write limit in order
         * to accept this single record.
         */
        public boolean hasRoomFor(byte[] key, byte[] value) {
            if (isFull())
                return false;
            return this.compressor.numRecordsWritten() == 0 ?
                    this.initialCapacity >= LogBuffer.LOG_OVERHEAD + Record.recordSize(magic, key, value) :
                    this.writeLimit >= this.compressor.estimatedBytesWritten() + LogBuffer.LOG_OVERHEAD + Record.recordSize(magic, key, value);
        }

        public boolean isClosed() {
            return records != null;
        }

        public boolean isFull() {
            return isClosed() || this.writeLimit <= this.compressor.estimatedBytesWritten();
        }

        public int sizeInBytes() {
            return records != null ? records.sizeInBytes() : compressor.buffer().position();
        }

        public double compressionRate() {
            return compressor.compressionRate();
        }
    }

}
