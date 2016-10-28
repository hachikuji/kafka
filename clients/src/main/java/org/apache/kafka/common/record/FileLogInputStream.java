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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.CorruptRecordException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A log input stream which is backed by a {@link FileChannel}. This input stream provides
 * an "eager" mode in which records are written to ByteBuffer objects immediately when creating
 * the log entry and a "lazy" mode which delays the read of the record until it is actually requested.
 * @param <T> The type of the log entry (see {@link FileChannelLogEntry below}.
 */
abstract class FileLogInputStream<T extends LogEntry> implements LogInputStream<T> {
    private long position;
    protected final long end;
    protected final FileChannel channel;
    private final int maxMessageSize;
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(LogBuffer.LOG_OVERHEAD);

    public FileLogInputStream(FileChannel channel, int maxMessageSize, long start, long end) {
        this.channel = channel;
        this.maxMessageSize = maxMessageSize;
        this.position = start;
        this.end = end;
    }

    protected abstract T nextEntry(long offset, long position, int size) throws IOException;

    public T nextEntry() throws IOException {
        if (position + LogBuffer.LOG_OVERHEAD >= end)
            return null;

        logHeaderBuffer.rewind();
        channel.read(logHeaderBuffer, position);
        if (logHeaderBuffer.hasRemaining())
            return null;

        logHeaderBuffer.rewind();
        long offset = logHeaderBuffer.getLong();
        int size = logHeaderBuffer.getInt();

        if (size < Record.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Message size is smaller than minimum record overhead (%d).", Record.RECORD_OVERHEAD_V0));

        if (size > maxMessageSize)
            throw new CorruptRecordException(String.format("Message size exceeds the largest allowable message size (%d).", maxMessageSize));

        if (position + LogBuffer.LOG_OVERHEAD + size > end)
            return null;

        T logEntry = nextEntry(offset, position, size);
        if (logEntry == null)
            return null;

        position += logEntry.size();
        return logEntry;
    }

    private static class LazyFileLogInputStream extends FileLogInputStream<FileChannelLogEntry> {

        public LazyFileLogInputStream(FileChannel channel, int maxMessageSize, long start, long end) {
            super(channel, maxMessageSize, start, end);
        }

        @Override
        protected FileChannelLogEntry nextEntry(long offset, long position, int size) throws IOException {
            return new FileChannelLogEntry(offset, channel, position, size);
        }
    }

    private static class EagerFileLogInputStream extends FileLogInputStream<LogEntry> {

        public EagerFileLogInputStream(FileChannel channel, int maxMessageSize, long start, long end) {
            super(channel, maxMessageSize, start, end);
        }

        protected LogEntry nextEntry(long offset, long position, int size) throws IOException {
            ByteBuffer recordBuffer = ByteBuffer.allocate(size);
            channel.read(recordBuffer, position + LogBuffer.LOG_OVERHEAD);
            if (recordBuffer.hasRemaining())
                return null;
            recordBuffer.rewind();
            return LogEntry.create(offset, new Record(recordBuffer));
        }

    }

    /**
     * Get an eager log input stream. Records will be written to a ByteBuffer as log entries
     * are parsed.
     * @param channel The underlying file channel
     * @param maxMessageSize The maximum allowed message size
     * @param start The starting position in the channel
     * @param end The end position in the channel
     * @return The input stream
     */
    public static LogInputStream<LogEntry> eagerInputStream(FileChannel channel,
                                                            int maxMessageSize,
                                                            long start,
                                                            long end) {
        return new EagerFileLogInputStream(channel, maxMessageSize, start, end);
    }

    /**
     * Get an eager log input stream. Records will not be read from the file until
     * the {@link LogEntry#record()} method is invoked on a returned instance.
     * @param channel The underlying file channel
     * @param maxMessageSize The maximum allowed message size
     * @param start The starting position in the channel
     * @param end The end position in the channel
     * @return The input stream
     */
    public static LogInputStream<FileChannelLogEntry> lazyInputStream(FileChannel channel,
                                                                      int maxMessageSize,
                                                                      long start,
                                                                      long end) {
        return new LazyFileLogInputStream(channel, maxMessageSize, start, end);
    }

    /**
     * Log entry backed by an underlying FileChannel. This allows iteration over the shallow log
     * entries without needing to read the record data into memory until it is needed. See
     * {@link FileLogBuffer#hasMatchingShallowMagic(byte)} for example usage.
     */
    public static class FileChannelLogEntry extends LogEntry {
        private final long offset;
        private final FileChannel channel;
        private final long position;
        private final int recordSize;
        private Record record = null;

        public FileChannelLogEntry(long offset, FileChannel channel, long position, int recordSize) {
            this.offset = offset;
            this.channel = channel;
            this.position = position;
            this.recordSize = recordSize;
        }

        @Override
        public long offset() {
            return offset;
        }

        public long position() {
            return position;
        }

        public byte magic() {
            if (record != null)
                return record.magic();

            try {
                byte[] magic = new byte[1];
                ByteBuffer buf = ByteBuffer.wrap(magic);
                channel.read(buf, position + LogBuffer.LOG_OVERHEAD + Record.MAGIC_OFFSET);
                if (buf.hasRemaining())
                    throw new KafkaException("Failed to read magic byte from FileChannel " + channel);
                return magic[0];
            } catch (IOException e) {
                throw new KafkaException(e);
            }
        }

        @Override
        public Record record() {
            if (record != null)
                return record;

            ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize);
            try {
                channel.read(recordBuffer, position + LogBuffer.LOG_OVERHEAD);
                if (recordBuffer.hasRemaining())
                    throw new KafkaException("Failed to read full record from channel " + channel);
            } catch (IOException e) {
                throw new KafkaException(e);
            }

            recordBuffer.rewind();
            record = new Record(recordBuffer);
            return record;
        }

        @Override
        public int size() {
            return LogBuffer.LOG_OVERHEAD + recordSize;
        }

    }
}
