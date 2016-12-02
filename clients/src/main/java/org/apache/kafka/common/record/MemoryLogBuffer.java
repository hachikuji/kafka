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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link LogBuffer} implementation backed by a ByteBuffer. This is used only for reading or
 * modifying in-place an existing buffer of log entries. To create a new buffer see {@link MemoryLogBufferBuilder},
 * or one of the {@link #builder(ByteBuffer, byte, CompressionType, TimestampType) builder} variants.
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
        dup.position((int) position);
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
        Iterator<LogEntry.ShallowLogEntry> iterator = shallowIterator();
        while (iterator.hasNext())
            bytes += iterator.next().sizeInBytes();
        return bytes;
    }

    /**
     * Filter this log buffer into the provided ByteBuffer.
     * @param filter The filter function
     * @param buffer The byte buffer to write the filtered records to
     * @return A FilterResult with a summary of the output (for metrics)
     */
    public FilterResult filterTo(LogRecordFilter filter, ByteBuffer buffer) {
        long firstOffset = -1;
        long maxTimestamp = Record.NO_TIMESTAMP;
        long offsetOfMaxTimestamp = -1L;
        int messagesRead = 0;
        int bytesRead = 0;
        int messagesRetained = 0;
        int bytesRetained = 0;

        Iterator<LogEntry.ShallowLogEntry> shallowIterator = shallowIterator();
        while (shallowIterator.hasNext()) {
            LogEntry.ShallowLogEntry entry = shallowIterator.next();
            bytesRead += entry.sizeInBytes();

            // We use the absolute offset to decide whether to retain the message or not Due KAFKA-4298, we have to
            // allow for the possibility that a previous version corrupted the log by writing a compressed message
            // set with a wrapper magic value not matching the magic of the inner messages. This will be fixed as we
            // recopy the messages to the destination buffer.

            byte shallowMagic = entry.magic();
            boolean writeOriginalEntry = true;
            List<LogRecord> retainedRecords = new ArrayList<>();

            for (LogRecord deepRecord : entry) {
                if (firstOffset < 0)
                    firstOffset = deepRecord.offset();

                messagesRead += 1;

                if (filter.shouldRetain(deepRecord)) {
                    // Check for log corruption due to KAFKA-4298. If we find it, make sure that we overwrite
                    // the corrupted entry with correct data.
                    if (!deepRecord.hasMagic(shallowMagic))
                        writeOriginalEntry = false;

                    retainedRecords.add(deepRecord);

                    // We need the max timestamp and last offset for time index
                    if (deepRecord.timestamp() > maxTimestamp)
                        maxTimestamp = deepRecord.timestamp();
                } else {
                    writeOriginalEntry = false;
                }
            }

            if (!retainedRecords.isEmpty())
                offsetOfMaxTimestamp = retainedRecords.get(retainedRecords.size() - 1).offset();

            if (writeOriginalEntry) {
                // There are no messages compacted out and no message format conversion, write the original message set back
                entry.writeTo(buffer);
                messagesRetained += retainedRecords.size();
                bytesRetained += entry.sizeInBytes();
            } else if (!retainedRecords.isEmpty()) {
                ByteBuffer slice = buffer.slice();
                TimestampType timestampType = entry.timestampType();
                long logAppendTime = timestampType == TimestampType.LOG_APPEND_TIME ? entry.timestamp() : -1L;
                MemoryLogBufferBuilder builder = builderWithRecords(slice, false, entry.magic(), firstOffset,
                        timestampType, entry.compressionType(), logAppendTime, retainedRecords);
                int bytesWritten = builder.build().sizeInBytes();
                buffer.position(buffer.position() + bytesWritten);
                messagesRetained += retainedRecords.size();
                bytesRetained += bytesWritten;
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
    public Iterator<LogEntry.ShallowLogEntry> shallowIterator() {
        return LogBufferIterator.shallowIterator(new ByteBufferLogInputStream(buffer.duplicate(), Integer.MAX_VALUE));
    }

    @Override
    public Iterator<LogEntry> deepIterator() {
        return deepEntries(false);
    }

    public Iterator<LogEntry> deepEntries(boolean ensureMatchingMagic) {
        return deepEntries(ensureMatchingMagic, Integer.MAX_VALUE);
    }

    private Iterator<LogEntry> deepEntries(boolean ensureMatchingMagic, int maxMessageSize) {
        return new LogBufferIterator(new ByteBufferLogInputStream(buffer.duplicate(), maxMessageSize), false,
                ensureMatchingMagic, maxMessageSize);
    }

    @Override
    public String toString() {
        Iterator<LogRecord> iter = records();
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        while (iter.hasNext()) {
            LogRecord record = iter.next();
            builder.append('(');
            builder.append("record=");
            builder.append(record);
            builder.append(")");
            if (iter.hasNext())
                builder.append(", ");
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

    public interface LogRecordFilter {
        boolean shouldRetain(LogRecord record);
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

    public static MemoryLogBufferBuilder builder(ByteBuffer buffer,
                                                 CompressionType compressionType,
                                                 TimestampType timestampType,
                                                 int writeLimit) {
        return new MemoryLogBufferBuilder(buffer, Record.CURRENT_MAGIC_VALUE, compressionType, timestampType, 0L,
                System.currentTimeMillis(), 0L, (short) 0, 0, writeLimit);
    }

    public static MemoryLogBufferBuilder builder(ByteBuffer buffer,
                                                 byte magic,
                                                 CompressionType compressionType,
                                                 TimestampType timestampType,
                                                 int writeLimit) {
        return new MemoryLogBufferBuilder(buffer, magic, compressionType, timestampType, 0L,
                System.currentTimeMillis(), 0L, (short) 0, 0, writeLimit);
    }

    public static MemoryLogBufferBuilder builder(ByteBuffer buffer,
                                                 byte magic,
                                                 CompressionType compressionType,
                                                 TimestampType timestampType,
                                                 long baseOffset,
                                                 long logAppendTime) {
        return new MemoryLogBufferBuilder(buffer, magic, compressionType, timestampType, baseOffset,
                logAppendTime, 0L, (short) 0, 0, buffer.capacity());
    }

    public static MemoryLogBufferBuilder builder(ByteBuffer buffer,
                                                 CompressionType compressionType,
                                                 TimestampType timestampType) {
        // use the buffer capacity as the default write limit
        return builder(buffer, compressionType, timestampType, buffer.capacity());
    }

    public static MemoryLogBufferBuilder builder(ByteBuffer buffer,
                                                 byte magic,
                                                 CompressionType compressionType,
                                                 TimestampType timestampType) {
        return builder(buffer, magic, compressionType, timestampType, 0L);
    }

    public static MemoryLogBufferBuilder builder(ByteBuffer buffer,
                                                 byte magic,
                                                 CompressionType compressionType,
                                                 TimestampType timestampType,
                                                 long baseOffset) {
        return builder(buffer, magic, compressionType, timestampType, baseOffset, System.currentTimeMillis());
    }

    public static MemoryLogBufferBuilder builder(ByteBuffer buffer,
                                                 byte magic,
                                                 CompressionType compressionType,
                                                 TimestampType timestampType,
                                                 long baseOffset,
                                                 long pid,
                                                 short epoch,
                                                 int baseSequence) {
        return new MemoryLogBufferBuilder(buffer, magic, compressionType, timestampType, baseOffset,
                System.currentTimeMillis(), pid, epoch, baseSequence, buffer.capacity());
    }


    public static MemoryLogBuffer readableRecords(ByteBuffer buffer) {
        return new MemoryLogBuffer(buffer);
    }

    public static MemoryLogBuffer withLogEntries(CompressionType compressionType, List<LogEntry> entries) {
        return withLogEntries(TimestampType.CREATE_TIME, compressionType, System.currentTimeMillis(), entries);
    }

    public static MemoryLogBuffer withLogRecords(byte magic, CompressionType compressionType, TimestampType timestampType, List<LogRecord> records) {
        return withLogRecords(magic, timestampType, compressionType, System.currentTimeMillis(), records);
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

    private static MemoryLogBuffer withLogRecords(byte magic,
                                                  TimestampType timestampType,
                                                  CompressionType compressionType,
                                                  long logAppendTime,
                                                  List<LogRecord> records) {
        if (records.isEmpty())
            return MemoryLogBuffer.EMPTY;
        long firstOffset = records.get(0).offset();
        return builderWithRecords(false, magic, firstOffset, timestampType, compressionType, logAppendTime, records).build();
    }

    private static List<LogEntry> buildLogEntries(long initialOffset, List<Record> records) {
        List<LogEntry> entries = new ArrayList<>();
        for (Record record : records)
            entries.add(LogEntry.create(initialOffset++, record));
        return entries;
    }

    public static MemoryLogBufferBuilder builderWithEntries(TimestampType timestampType,
                                                            CompressionType compressionType,
                                                            long logAppendTime,
                                                            List<LogEntry> entries) {
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize(compressionType, entries));
        return builderWithEntries(buffer, timestampType, compressionType, logAppendTime, entries);
    }

    public static MemoryLogBufferBuilder builderWithRecords(boolean assignOffsets,
                                                            byte magic,
                                                            long firstOffset,
                                                            TimestampType timestampType,
                                                            CompressionType compressionType,
                                                            long logAppendTime,
                                                            List<LogRecord> records) {
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSizeRecords(compressionType, records));
        return builderWithRecords(buffer, assignOffsets, magic, firstOffset, timestampType, compressionType, logAppendTime, records);
    }

    private static MemoryLogBufferBuilder builderWithRecords(ByteBuffer buffer,
                                                             boolean assignOffsets,
                                                             byte magic,
                                                             long firstOffset,
                                                             TimestampType timestampType,
                                                             CompressionType compressionType,
                                                             long logAppendTime,
                                                             List<LogRecord> records) {
        if (records.isEmpty())
            throw new IllegalArgumentException();

        MemoryLogBufferBuilder builder = builder(buffer, magic, compressionType, timestampType, firstOffset, logAppendTime);
        long offset = firstOffset;
        for (LogRecord record : records) {
            if (assignOffsets)
                builder.append(offset++, record.timestamp(), record.key(), record.value());
            else
                builder.append(record.offset(), record.timestamp(), record.key(), record.value());
        }
        return builder;
    }


    private static MemoryLogBufferBuilder builderWithEntries(ByteBuffer buffer,
                                                             TimestampType timestampType,
                                                             CompressionType compressionType,
                                                             long logAppendTime,
                                                             List<LogEntry> entries) {
        if (entries.isEmpty())
            throw new IllegalArgumentException();

        LogEntry firstEntry = entries.iterator().next();
        long firstOffset = firstEntry.offset();
        byte magic = firstEntry.magic();

        MemoryLogBufferBuilder builder = builder(buffer, magic, compressionType, timestampType, firstOffset, logAppendTime);
        for (LogEntry entry : entries)
            builder.append(entry);

        return builder;
    }

}
