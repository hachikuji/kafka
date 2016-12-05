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
 * A log input stream which is backed by a {@link FileChannel}.
 */
public class FileLogInputStream implements LogInputStream<FileLogInputStream.FileChannelLogEntry> {
    private long position;
    protected final long end;
    protected final FileChannel channel;
    private final int maxRecordSize;
    private final boolean eagerLoadRecords;
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(LogBuffer.LOG_OVERHEAD);

    /**
     * Create a new log input stream over the FileChannel
     * @param channel Underlying FileChannel
     * @param maxRecordSize Maximum size of records
     * @param start Position in the file channel to start from
     * @param end Position in the file channel not to read past
     * @param eagerLoadRecords Whether or not to load records eagerly (i.e. in {@link #nextEntry()})
     */
    public FileLogInputStream(FileChannel channel, int maxRecordSize, long start, long end, boolean eagerLoadRecords) {
        this.channel = channel;
        this.maxRecordSize = maxRecordSize;
        this.position = start;
        this.end = end;
        this.eagerLoadRecords = eagerLoadRecords;
    }

    @Override
    public FileChannelLogEntry nextEntry() throws IOException {
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
            throw new CorruptRecordException(String.format("Record size is smaller than minimum record overhead (%d).", Record.RECORD_OVERHEAD_V0));

        if (size > maxRecordSize)
            throw new CorruptRecordException(String.format("Record size exceeds the largest allowable message size (%d).", maxRecordSize));

        if (position + LogBuffer.LOG_OVERHEAD + size > end)
            return null;

        FileChannelLogEntry logEntry = new FileChannelLogEntry(offset, channel, position, size);
        if (eagerLoadRecords)
            logEntry.loadRecord();

        position += logEntry.size();
        return logEntry;
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

        public FileChannelLogEntry(long offset,
                                   FileChannel channel,
                                   long position,
                                   int recordSize) {
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

        private Record loadRecord() throws IOException {
            if (record != null)
                return record;

            ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize);
            channel.read(recordBuffer, position + LogBuffer.LOG_OVERHEAD);
            if (recordBuffer.hasRemaining())
                throw new IOException("Failed to read full record from channel " + channel);

            recordBuffer.rewind();
            record = new Record(recordBuffer);
            return record;
        }

        @Override
        public Record record() {
            if (record != null)
                return record;

            try {
                return loadRecord();
            } catch (IOException e) {
                throw new KafkaException(e);
            }
        }

        @Override
        public int size() {
            return LogBuffer.LOG_OVERHEAD + recordSize;
        }

    }
}
