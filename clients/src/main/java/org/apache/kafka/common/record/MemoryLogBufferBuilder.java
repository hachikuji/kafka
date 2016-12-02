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

import org.apache.kafka.common.KafkaException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * This class is used to write new log data in memory, i.e. this is the write path for {@link MemoryLogBuffer}.
 * It transparently handles compression and exposes methods for appending new entries, possibly with message
 * format conversion.
 */
public class MemoryLogBufferBuilder {

    static private final float COMPRESSION_RATE_DAMPING_FACTOR = 0.9f;
    static private final float COMPRESSION_RATE_ESTIMATION_FACTOR = 1.05f;
    static private final int COMPRESSION_DEFAULT_BUFFER_SIZE = 1024;

    private static final float[] TYPE_TO_RATE;

    static {
        int maxTypeId = -1;
        for (CompressionType type : CompressionType.values())
            maxTypeId = Math.max(maxTypeId, type.id);
        TYPE_TO_RATE = new float[maxTypeId + 1];
        for (CompressionType type : CompressionType.values()) {
            TYPE_TO_RATE[type.id] = type.rate;
        }
    }

    // dynamically load the snappy and lz4 classes to avoid runtime dependency if we are not using compression
    // caching constructors to avoid invoking of Class.forName method for each batch
    private static MemoizingConstructorSupplier snappyOutputStreamSupplier = new MemoizingConstructorSupplier(new ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
            return Class.forName("org.xerial.snappy.SnappyOutputStream")
                .getConstructor(OutputStream.class, Integer.TYPE);
        }
    });

    private static MemoizingConstructorSupplier lz4OutputStreamSupplier = new MemoizingConstructorSupplier(new ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
            return Class.forName("org.apache.kafka.common.record.KafkaLZ4BlockOutputStream")
                .getConstructor(OutputStream.class, Boolean.TYPE);
        }
    });

    private static MemoizingConstructorSupplier snappyInputStreamSupplier = new MemoizingConstructorSupplier(new ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
            return Class.forName("org.xerial.snappy.SnappyInputStream")
                .getConstructor(InputStream.class);
        }
    });

    private static MemoizingConstructorSupplier lz4InputStreamSupplier = new MemoizingConstructorSupplier(new ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
            return Class.forName("org.apache.kafka.common.record.KafkaLZ4BlockInputStream")
                .getConstructor(InputStream.class, Boolean.TYPE);
        }
    });

    private final ByteBuffer initialBuffer;
    private final TimestampType timestampType;
    private final CompressionType compressionType;
    private final DataOutputStream appendStream;
    private final ByteBufferOutputStream bufferStream;
    private final byte magic;
    private final int initPos;
    private final long baseOffset;
    private final long logAppendTime;
    private final long pid;
    private final short epoch;
    private final int baseSequence;
    private final int writeLimit;

    private MemoryLogBuffer builtLogBuffer;
    private long writtenUncompressed;
    private long numRecords;
    private float compressionRate;
    private long maxTimestamp;
    private long offsetOfMaxTimestamp;
    private long lastOffset = -1;

    public MemoryLogBufferBuilder(ByteBuffer buffer,
                                  byte magic,
                                  CompressionType compressionType,
                                  TimestampType timestampType,
                                  long baseOffset,
                                  long logAppendTime,
                                  long pid,
                                  short epoch,
                                  int baseSequence,
                                  int writeLimit) {
        this.magic = magic;
        this.timestampType = timestampType;
        this.compressionType = compressionType;
        this.baseOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.initPos = buffer.position();
        this.numRecords = 0;
        this.writtenUncompressed = 0;
        this.compressionRate = 1;
        this.maxTimestamp = Record.NO_TIMESTAMP;
        this.pid = pid;
        this.epoch = epoch;
        this.baseSequence = baseSequence;
        this.writeLimit = writeLimit;
        this.initialBuffer = buffer;

        if (magic > 1) {
            buffer.position(initPos + EosLogEntry.RECORDS_OFFSET);
        } else if (compressionType != CompressionType.NONE) {
            // for compressed records, leave space for the header and the shallow message metadata
            // and move the starting position to the value payload offset
            buffer.position(initPos + LogBuffer.LOG_OVERHEAD + Record.recordOverhead(magic));
        }

        // create the stream
        bufferStream = new ByteBufferOutputStream(buffer);
        appendStream = wrapForOutput(bufferStream, compressionType, magic, COMPRESSION_DEFAULT_BUFFER_SIZE);
    }

    public ByteBuffer initialBuffer() {
        return initialBuffer;
    }

    private ByteBuffer buffer() {
        return bufferStream.buffer();
    }

    public double compressionRate() {
        return compressionRate;
    }

    /**
     * Close this builder and return the resulting buffer.
     * @return The built log buffer
     */
    public MemoryLogBuffer build() {
        close();
        return builtLogBuffer;
    }

    /**
     * Get the max timestamp and its offset. If the log append time is used, then the offset will
     * be either the first offset in the set if no compression is used or the last offset otherwise.
     * @return The max timestamp and its offset
     */
    public MemoryLogBuffer.RecordsInfo info() {
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            long offsetOfMaxTimestamp = compressionType == CompressionType.NONE ? baseOffset : lastOffset;
            return new MemoryLogBuffer.RecordsInfo(logAppendTime,  offsetOfMaxTimestamp);
        }
        return new MemoryLogBuffer.RecordsInfo(maxTimestamp, offsetOfMaxTimestamp);
    }

    public void close() {
        if (builtLogBuffer != null)
            return;

        try {
            appendStream.close();
        } catch (IOException e) {
            throw new KafkaException(e);
        }

        if (magic > 1)
            writeEosEntryHeader();
        else if (compressionType != CompressionType.NONE)
            writerCompressedWrapperHeader();

        ByteBuffer buffer = buffer().duplicate();
        buffer.flip();
        buffer.position(initPos);
        builtLogBuffer = MemoryLogBuffer.readableRecords(buffer);
    }

    private void writeEosEntryHeader() {
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initPos);

        long timestamp = timestampType == TimestampType.LOG_APPEND_TIME ? logAppendTime : maxTimestamp;
        int size = pos - initPos;

        int offsetDelta = (int) (lastOffset - baseOffset);
        EosLogEntry.writeInPlaceHeader(buffer, baseOffset, offsetDelta, size, magic, compressionType, timestampType,
                timestamp, pid, epoch, baseSequence);

        buffer.position(pos);
    }

    private void writerCompressedWrapperHeader() {
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initPos);

        int wrapperSize = pos - initPos - LogBuffer.LOG_OVERHEAD;
        LogEntry.writeHeader(buffer, lastOffset, wrapperSize);

        long timestamp = timestampType == TimestampType.LOG_APPEND_TIME ? logAppendTime : maxTimestamp;
        Record.writeCompressedRecordHeader(buffer, magic, wrapperSize, timestamp, compressionType, timestampType);

        buffer.position(pos);

        // update the compression ratio
        this.compressionRate = (float) buffer.position() / this.writtenUncompressed;
        TYPE_TO_RATE[compressionType.id] = TYPE_TO_RATE[compressionType.id] * COMPRESSION_RATE_DAMPING_FACTOR +
            compressionRate * (1 - COMPRESSION_RATE_DAMPING_FACTOR);
    }

    /**
     * Append a new record and offset to the buffer
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return crc of the record
     */
    public long append(long offset, long timestamp, ByteBuffer key, ByteBuffer value) {
        try {
            if (lastOffset > 0 && offset <= lastOffset)
                throw new IllegalArgumentException(String.format("Illegal offset %s following previous offset %s (Offsets must increase monotonically).", offset, lastOffset));

            if (magic > 1)
                return appendEosRecord(offset, timestamp, key, value);

            if (compressionType == CompressionType.NONE && timestampType == TimestampType.LOG_APPEND_TIME)
                timestamp = logAppendTime;

            int size = Record.recordSize(magic, key, value);
            LogEntry.writeHeader(appendStream, toInnerOffset(offset), size);

            if (timestampType == TimestampType.LOG_APPEND_TIME)
                timestamp = logAppendTime;
            long crc = Record.write(appendStream, magic, timestamp, key, value, CompressionType.NONE, timestampType);
            recordWritten(offset, timestamp, size + LogBuffer.LOG_OVERHEAD);
            return crc;
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    public long append(long offset, long timestamp, byte[] key, byte[] value) {
        return append(offset, timestamp, wrapNullable(key), wrapNullable(value));
    }

    private long appendEosRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value) throws IOException {
        long crc = EosLogRecord.write(appendStream, offset - baseOffset, (byte) 0, timestamp, key, value);
        recordWritten(offset, timestamp, EosLogRecord.sizeOf(key, value) + LogBuffer.LOG_OVERHEAD);
        return crc;
    }

    /**
     * Add the record, converting to the desired magic value if necessary.
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void convertAndAppend(long offset, Record record) {
        if (magic == record.magic()) {
            append(offset, record);
            return;
        }

        if (lastOffset > 0 && offset <= lastOffset)
            throw new IllegalArgumentException(String.format("Illegal offset %s following previous offset %s (Offsets must increase monotonically).", offset, lastOffset));

        try {
            int size = record.convertedSize(magic);
            LogEntry.writeHeader(appendStream, toInnerOffset(offset), size);
            long timestamp = timestampType == TimestampType.LOG_APPEND_TIME ? logAppendTime : record.timestamp();
            record.convertTo(appendStream, magic, timestamp, timestampType);
            recordWritten(offset, timestamp, size);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Add a record without doing offset/magic validation (this should only be used in testing).
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendUnchecked(long offset, Record record) {
        try {
            int size = record.size();
            LogEntry.writeHeader(appendStream, toInnerOffset(offset), size);

            ByteBuffer buffer = record.buffer().duplicate();
            appendStream.write(buffer.array(), buffer.arrayOffset(), buffer.limit());

            recordWritten(offset, record.timestamp(), size);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Append the given log entry. The entry's record must have a magic which matches the magic use to
     * construct this builder and the offset must be greater than the last appended entry.
     * @param entry The entry to append
     */
    public void append(LogEntry entry) {
        append(entry.offset(), entry.record());
    }

    /**
     *
     * @param record
     */
    public void append(LogRecord record) {
        append(record.offset(), record.timestamp(), record.key(), record.value());
    }

    /**
     * Append a log record using a different offset
     * @param offset
     * @param record
     */
    public void append(long offset, LogRecord record) {
        append(offset, record.timestamp(), record.key(), record.value());
    }


    /**
     * Add a record with a given offset. The record must have a magic which matches the magic use to
     * construct this builder and the offset must be greater than the last appended entry.
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void append(long offset, Record record) {
        if (record.magic() != magic)
            throw new IllegalArgumentException("Inner log entries must have matching magic values as the wrapper");
        if (lastOffset > 0 && offset <= lastOffset)
            throw new IllegalArgumentException(String.format("Illegal offset %s following previous offset %s (Offsets must increase monotonically).", offset, lastOffset));
        appendUnchecked(offset, record);
    }

    private long toInnerOffset(long offset) {
        // use relative offsets for compressed messages with magic v1
        if (magic > 0 && compressionType != CompressionType.NONE)
            return offset - baseOffset;
        return offset;
    }

    private void recordWritten(long offset, long timestamp, int size) {
        numRecords += 1;
        writtenUncompressed += size;
        lastOffset = offset;

        if (timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
            offsetOfMaxTimestamp = offset;
        }
    }

    /**
     * Get an estimate of the number of bytes written (based on the estimation factor hard-coded in {@link CompressionType}.
     * @return The estimated number of bytes written
     */
    private long estimatedBytesWritten() {
        if (compressionType == CompressionType.NONE) {
            return buffer().position();
        } else {
            // estimate the written bytes to the underlying byte buffer based on uncompressed written bytes
            return (long) (writtenUncompressed * TYPE_TO_RATE[compressionType.id] * COMPRESSION_RATE_ESTIMATION_FACTOR);
        }
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
        return !isFull() && (numRecords == 0 ?
                this.initialBuffer.capacity() >= LogBuffer.LOG_OVERHEAD + Record.recordSize(magic, key, value) :
                this.writeLimit >= estimatedBytesWritten() + LogBuffer.LOG_OVERHEAD + Record.recordSize(magic, key, value));
    }

    public boolean isClosed() {
        return builtLogBuffer != null;
    }

    public boolean isFull() {
        return isClosed() || this.writeLimit <= estimatedBytesWritten();
    }

    public int sizeInBytes() {
        return builtLogBuffer != null ? builtLogBuffer.sizeInBytes() : buffer().position();
    }

    private static DataOutputStream wrapForOutput(ByteBufferOutputStream buffer, CompressionType type, byte messageVersion, int bufferSize) {
        try {
            switch (type) {
                case NONE:
                    return buffer;
                case GZIP:
                    return new DataOutputStream(new GZIPOutputStream(buffer, bufferSize));
                case SNAPPY:
                    try {
                        OutputStream stream = (OutputStream) snappyOutputStreamSupplier.get().newInstance(buffer, bufferSize);
                        return new DataOutputStream(stream);
                    } catch (Exception e) {
                        throw new KafkaException(e);
                    }
                case LZ4:
                    try {
                        OutputStream stream = (OutputStream) lz4OutputStreamSupplier.get().newInstance(buffer,
                                messageVersion == Record.MAGIC_VALUE_V0);
                        return new DataOutputStream(stream);
                    } catch (Exception e) {
                        throw new KafkaException(e);
                    }
                default:
                    throw new IllegalArgumentException("Unknown compression type: " + type);
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public static DataInputStream wrapForInput(ByteBufferInputStream buffer, CompressionType type, byte messageVersion) {
        try {
            switch (type) {
                case NONE:
                    return buffer;
                case GZIP:
                    return new DataInputStream(new GZIPInputStream(buffer));
                case SNAPPY:
                    try {
                        InputStream stream = (InputStream) snappyInputStreamSupplier.get().newInstance(buffer);
                        return new DataInputStream(stream);
                    } catch (Exception e) {
                        throw new KafkaException(e);
                    }
                case LZ4:
                    try {
                        InputStream stream = (InputStream) lz4InputStreamSupplier.get().newInstance(buffer,
                                messageVersion == Record.MAGIC_VALUE_V0);
                        return new DataInputStream(stream);
                    } catch (Exception e) {
                        throw new KafkaException(e);
                    }
                default:
                    throw new IllegalArgumentException("Unknown compression type: " + type);
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    private interface ConstructorSupplier {
        Constructor get() throws ClassNotFoundException, NoSuchMethodException;
    }

    // this code is based on Guava's @see{com.google.common.base.Suppliers.MemoizingSupplier}
    private static class MemoizingConstructorSupplier {
        final ConstructorSupplier delegate;
        transient volatile boolean initialized;
        transient Constructor value;

        public MemoizingConstructorSupplier(ConstructorSupplier delegate) {
            this.delegate = delegate;
        }

        public Constructor get() throws NoSuchMethodException, ClassNotFoundException {
            if (!initialized) {
                synchronized (this) {
                    if (!initialized) {
                        value = delegate.get();
                        initialized = true;
                    }
                }
            }
            return value;
        }
    }
}
