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
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.record.FileLogInputStream.FileChannelLogEntry;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An on-disk log buffer. An optional start and end position can be applied to the message set
 * which will allow slicing a subset of the file.File-backed log buffer.
 */
public class FileLogBuffer extends AbstractLogBuffer implements Closeable {
    private final boolean isSlice;
    private final FileChannel channel;
    private final long start;
    private final long end;
    private volatile File file;
    private final AtomicLong size;

    public FileLogBuffer(File file,
                         FileChannel channel,
                         long start,
                         long end,
                         boolean isSlice) throws IOException {
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.end = end;
        this.isSlice = isSlice;
        this.size = new AtomicLong();

        // set the initial size of the buffer
        resize();
    }

    public void resize() throws IOException {
        if (isSlice) {
            size.set(end - start);
        } else {
            size.set(Math.min(channel.size(), end) - start);

            // if this is not a slice, update the file pointer to the end of the file
            // set the file position to the last byte in the file
            channel.position(Math.min(channel.size(), end));
        }
    }

    @Override
    public int sizeInBytes() {
        return (int) size.get();
    }

    /**
     * Get the underlying file.
     * @return The file
     */
    public File file() {
        return file;
    }

    /**
     * Get the underlying file channel.
     * @return The file channel
     */
    public FileChannel channel() {
        return channel;
    }

    /**
     * Read log entries into a given buffer.
     * @param buffer The buffer to read into
     * @param position Position in the buffer to read from
     * @return The same buffer
     * @throws IOException
     */
    public ByteBuffer readInto(ByteBuffer buffer, int position) throws IOException {
        channel.read(buffer, position + this.start);
        buffer.flip();
        return buffer;
    }

    /**
     * Return a log buffer which is a view into this set starting from the given position and with the given size limit.
     *
     * If the size is beyond the end of the file, the end will be based on the size of the file at the time of the read.
     *
     * If this message set is already sliced, the position will be taken relative to that slicing.
     *
     * @param position The start position to begin the read from
     * @param size The number of bytes after the start position to include
     * @return A sliced wrapper on this message set limited based on the given position and size
     */
    public FileLogBuffer read(long position, int size) throws IOException {
        if (position < 0)
            throw new IllegalArgumentException("Invalid position: " + position);
        if (size < 0)
            throw new IllegalArgumentException("Invalid size: " + size);

        final long end;
        if (this.start + position + size < 0)
            end = sizeInBytes();
        else
            end = Math.min(this.start + position + size, sizeInBytes());
        return new FileLogBuffer(file, channel, this.start + position, end, true);
    }

    /**
     * Append log entries to the buffer
     * @param logBuffer The buffer containing the entries to append
     * @return the number of bytes written to the underlying file
     */
    public int append(MemoryLogBuffer logBuffer) throws IOException {
        int written = logBuffer.writeFullyTo(channel);
        size.getAndAdd(written);
        return written;
    }

    /**
     * Commit all written data to the physical disk
     */
    public void flush() throws IOException {
        channel.force(true);
    }

    /**
     * Close this message set
     */
    public void close() throws IOException {
        flush();
        trim();
        channel.close();
    }

    /**
     * Delete this message set from the filesystem
     * @return True iff this message set was deleted.
     */
    public boolean delete() {
        Utils.closeQuietly(channel, "FileChannel");
        return file.delete();
    }

    /**
     * Trim file when close or roll to next file
     */
    public void trim() throws IOException {
        truncateTo(sizeInBytes());
    }

    /**
     * Rename the file that backs this message set
     * @throws IOException if rename fails.
     */
    public void renameTo(File f) throws IOException {
        try {
            Utils.atomicMoveWithFallback(file.toPath(), f.toPath());
        }  finally {
            this.file = f;
        }
    }

    /**
     * Truncate this file message set to the given size in bytes. Note that this API does no checking that the
     * given size falls on a valid message boundary.
     * In some versions of the JDK truncating to the same size as the file message set will cause an
     * update of the files mtime, so truncate is only performed if the targetSize is smaller than the
     * size of the underlying FileChannel.
     * It is expected that no other threads will do writes to the log when this function is called.
     * @param targetSize The size to truncate to. Must be between 0 and sizeInBytes.
     * @return The number of bytes truncated off
     */
    public int truncateTo(int targetSize) throws IOException {
        int originalSize = sizeInBytes();
        if (targetSize > originalSize || targetSize < 0)
            throw new KafkaException("Attempt to truncate log segment to " + targetSize + " bytes failed, " +
                    " size of this log segment is " + originalSize + " bytes.");
        if (targetSize < Long.valueOf(channel.size()).intValue()) {
            channel.truncate(targetSize);
            channel.position(targetSize);
            size.set(targetSize);
        }
        return originalSize - targetSize;
    }

    @Override
    public long writeTo(GatheringByteChannel destChannel, long offset, int length) throws IOException {
        long newSize = Math.min(channel.size(), end) - start;
        if (newSize < size.get())
            throw new KafkaException(String.format("Size of FileRecords %s has been truncated during write: old size %d, new size %d", file.getAbsolutePath(), size, newSize));

        long position = start + offset;
        long count = Math.min(length, this.size.get());
        final long bytesTransferred;
        if (destChannel instanceof TransportLayer) {
            TransportLayer tl = (TransportLayer) destChannel;
            bytesTransferred = tl.transferFrom(this.channel, position, count);
        } else {
            bytesTransferred = this.channel.transferTo(position, count, destChannel);
        }
        return bytesTransferred;
    }

    /**
     * Search forward for the file position of the last offset that is greater than or equal to the target offset
     * and return its physical position and the size of the message (including log overhead) at the returned offset. If
     * no such offsets are found, return null.
     *
     * @param targetOffset The offset to search for.
     * @param startingPosition The starting position in the file to begin searching from.
     */
    public LogEntryPosition searchForOffsetWithSize(long targetOffset, int startingPosition) {
        Iterator<FileChannelLogEntry> iterator = shallowEntriesFrom(startingPosition);
        while (iterator.hasNext()) {
            FileChannelLogEntry entry = iterator.next();
            long offset = entry.offset();
            if (offset >= targetOffset)
                return new LogEntryPosition(offset, entry.position(), entry.size());
        }
        return null;
    }

    /**
     * Search forward for the message whose timestamp is greater than or equals to the target timestamp.
     *
     * @param targetTimestamp The timestamp to search for.
     * @param startingPosition The starting position to search.
     * @return The timestamp and offset of the message found. None, if no message is found.
     */
    public TimestampAndOffset searchForTimestamp(long targetTimestamp, int startingPosition) {
        Iterator<FileChannelLogEntry> shallowIterator = shallowEntriesFrom(startingPosition);
        while (shallowIterator.hasNext()) {
            LogEntry shallowEntry = shallowIterator.next();
            Record shallowRecord = shallowEntry.record();
            if (shallowRecord.timestamp() >= targetTimestamp) {
                // We found a message
                for (LogEntry deepLogEntry : shallowEntry) {
                    long timestamp = deepLogEntry.record().timestamp();
                    if (timestamp >= targetTimestamp)
                        return new TimestampAndOffset(timestamp, deepLogEntry.offset());
                }
                throw new IllegalStateException(String.format("The message set (max timestamp = %s, max offset = %s" +
                        " should contain target timestamp $targetTimestamp but it does not.", shallowRecord.timestamp(),
                        shallowEntry.offset()));
            }
        }
        return null;
    }

    /**
     * Return the largest timestamp of the messages after a given position in this file message set.
     * @param startingPosition The starting position.
     * @return The largest timestamp of the messages after the given position.
     */
    public TimestampAndOffset largestTimestampAfter(int startingPosition) {
        long maxTimestamp = Record.NO_TIMESTAMP;
        long offsetOfMaxTimestamp = -1L;

        Iterator<FileChannelLogEntry> shallowIterator = shallowEntriesFrom(startingPosition);
        while (shallowIterator.hasNext()) {
            LogEntry shallowEntry = shallowIterator.next();
            long timestamp = shallowEntry.record().timestamp();
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
                offsetOfMaxTimestamp = shallowEntry.offset();
            }
        }
        return new TimestampAndOffset(maxTimestamp, offsetOfMaxTimestamp);
    }

    @Override
    public boolean hasMatchingShallowMagic(byte magic) {
        Iterator<FileChannelLogEntry> iterator = shallowEntriesFrom(Integer.MAX_VALUE, start, false);
        while (iterator.hasNext())
            if (iterator.next().magic() != magic)
                return false;
        return true;
    }

    @Override
    public Iterator<FileChannelLogEntry> shallowEntries() {
        return shallowEntriesFrom(start);
    }

    public Iterator<FileChannelLogEntry> shallowEntries(int maxMessageSize) {
        return shallowEntriesFrom(maxMessageSize, start, true);
    }

    private Iterator<FileChannelLogEntry> shallowEntriesFrom(long start) {
        return shallowEntriesFrom(Integer.MAX_VALUE, start, true);
    }

    private Iterator<FileChannelLogEntry> shallowEntriesFrom(int maxMessageSize, long start, boolean eagerLoadRecords) {
        final long end;
        if (isSlice)
            end = this.end;
        else
            end = this.sizeInBytes();
        FileLogInputStream inputStream = new FileLogInputStream(channel, maxMessageSize, start, end, eagerLoadRecords);
        return LogBufferIterator.shallowIterator(inputStream);
    }

    @Override
    public Iterator<LogEntry> deepEntries() {
        final long end;
        if (isSlice)
            end = this.end;
        else
            end = this.sizeInBytes();
        FileLogInputStream inputStream = new FileLogInputStream(channel, Integer.MAX_VALUE, start, end, true);
        return new LogBufferIterator(inputStream, false, false, Integer.MAX_VALUE);
    }

    public static FileLogBuffer open(File file,
                                     boolean mutable,
                                     boolean fileAlreadyExists,
                                     int initFileSize,
                                     boolean preallocate) throws IOException {
        FileChannel channel = openChannel(file, mutable, fileAlreadyExists, initFileSize, preallocate);
        int end = (!fileAlreadyExists && preallocate) ? 0 : Integer.MAX_VALUE;
        return new FileLogBuffer(file, channel, 0, end, false);
    }

    public static FileLogBuffer open(File file,
                                     boolean fileAlreadyExists,
                                     int initFileSize,
                                     boolean preallocate) throws IOException {
        return open(file, true, fileAlreadyExists, initFileSize, preallocate);
    }

    public static FileLogBuffer open(File file, boolean mutable) throws IOException {
        return open(file, mutable, false, 0, false);
    }

    public static FileLogBuffer open(File file) throws IOException {
        return open(file, true);
    }

    /**
     * Open a channel for the given file
     * For windows NTFS and some old LINUX file system, set preallocate to true and initFileSize
     * with one value (for example 512 * 1025 *1024 ) can improve the kafka produce performance.
     * @param file File path
     * @param mutable mutable
     * @param fileAlreadyExists File already exists or not
     * @param initFileSize The size used for pre allocate file, for example 512 * 1025 *1024
     * @param preallocate Pre allocate file or not, gotten from configuration.
     */
    private static FileChannel openChannel(File file,
                                           boolean mutable,
                                           boolean fileAlreadyExists,
                                           int initFileSize,
                                           boolean preallocate) throws IOException {
        if (mutable) {
            if (fileAlreadyExists) {
                return new RandomAccessFile(file, "rw").getChannel();
            } else {
                if (preallocate) {
                    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                    randomAccessFile.setLength(initFileSize);
                    return randomAccessFile.getChannel();
                } else {
                    return new RandomAccessFile(file, "rw").getChannel();
                }
            }
        } else {
            return new FileInputStream(file).getChannel();
        }
    }

    public static class LogEntryPosition {
        public final long offset;
        public final long position;
        public final int size;

        public LogEntryPosition(long offset, long position, int size) {
            this.offset = offset;
            this.position = position;
            this.size = size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LogEntryPosition that = (LogEntryPosition) o;

            if (offset != that.offset) return false;
            if (position != that.position) return false;
            return size == that.size;
        }

        @Override
        public int hashCode() {
            int result = (int) (offset ^ (offset >>> 32));
            result = 31 * result + (int) (position ^ (position >>> 32));
            result = 31 * result + size;
            return result;
        }
    }

    public static class TimestampAndOffset {
        public final long timestamp;
        public final long offset;

        public TimestampAndOffset(long timestamp, long offset) {
            this.timestamp = timestamp;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TimestampAndOffset that = (TimestampAndOffset) o;

            if (timestamp != that.timestamp) return false;
            return offset == that.offset;
        }

        @Override
        public int hashCode() {
            int result = (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + (int) (offset ^ (offset >>> 32));
            return result;
        }
    }

}
