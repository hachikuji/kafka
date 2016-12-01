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

import org.apache.kafka.common.record.ByteBufferLogInputStream.ByteBufferLogEntry;

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
        Iterator<ByteBufferLogEntry> iterator = shallowEntries();
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

        Iterator<ByteBufferLogEntry> shallowIterator = shallowEntries();
        while (shallowIterator.hasNext()) {
            ByteBufferLogEntry shallowEntry = shallowIterator.next();
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

            if (!retainedEntries.isEmpty())
                offsetOfMaxTimestamp = retainedEntries.get(retainedEntries.size() - 1).offset();

            if (writeOriginalEntry) {
                // There are no messages compacted out and no message format conversion, write the original message set back
                shallowEntry.writeTo(buffer);
                messagesRetained += retainedEntries.size();
                bytesRetained += shallowEntry.size();
            } else if (!retainedEntries.isEmpty()) {
                ByteBuffer slice = buffer.slice();
                MemoryLogBufferBuilder builder = builderWithEntries(slice, shallowRecord.timestampType(), shallowRecord.compressionType(),
                        shallowRecord.timestamp(), retainedEntries);
                MemoryLogBuffer logBuffer = builder.build();
                buffer.position(buffer.position() + slice.position());
                messagesRetained += retainedEntries.size();
                bytesRetained += logBuffer.sizeInBytes();
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
    public Iterator<ByteBufferLogEntry> shallowEntries() {
        return LogBufferIterator.shallowIterator(new ByteBufferLogInputStream(buffer.duplicate(), Integer.MAX_VALUE));
    }

    @Override
    public Iterator<LogEntry> deepEntries() {
        return deepEntries(false);
    }

    public Iterator<LogEntry> deepEntries(boolean ensureMatchingMagic) {
        return deepEntries(ensureMatchingMagic, Integer.MAX_VALUE);
    }

    public Iterator<LogEntry> deepEntries(boolean ensureMatchingMagic, int maxMessageSize) {
        return new LogBufferIterator(new ByteBufferLogInputStream(buffer.duplicate(), maxMessageSize), false,
                ensureMatchingMagic, maxMessageSize);
    }

    @Override
    public String toString() {
        Iterator<LogEntry> iter = deepEntries();
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

    public static MemoryLogBufferBuilder builder(ByteBuffer buffer,
                                                 CompressionType compressionType,
                                                 TimestampType timestampType,
                                                 int writeLimit) {
        return new MemoryLogBufferBuilder(buffer, Record.CURRENT_MAGIC_VALUE, compressionType, timestampType, 0L, System.currentTimeMillis(), writeLimit);
    }

    public static MemoryLogBufferBuilder builder(ByteBuffer buffer,
                                                 byte magic,
                                                 CompressionType compressionType,
                                                 TimestampType timestampType,
                                                 long baseOffset,
                                                 long logAppendTime) {
        return new MemoryLogBufferBuilder(buffer, magic, compressionType, timestampType, baseOffset, logAppendTime, buffer.capacity());
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

    public static MemoryLogBuffer readableRecords(ByteBuffer buffer) {
        return new MemoryLogBuffer(buffer);
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

    public static MemoryLogBufferBuilder builderWithEntries(TimestampType timestampType,
                                                            CompressionType compressionType,
                                                            long logAppendTime,
                                                            List<LogEntry> entries) {
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize(compressionType, entries));
        return builderWithEntries(buffer, timestampType, compressionType, logAppendTime, entries);
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
        byte magic = firstEntry.record().magic();

        MemoryLogBufferBuilder builder = MemoryLogBuffer.builder(buffer, magic, compressionType, timestampType,
                firstOffset, logAppendTime);
        for (LogEntry entry : entries)
            builder.append(entry);

        return builder;
    }

}
