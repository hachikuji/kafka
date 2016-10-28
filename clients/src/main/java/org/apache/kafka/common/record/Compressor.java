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

public class Compressor {

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

    private final TimestampType timestampType;
    private final CompressionType compressionType;
    private final DataOutputStream appendStream;
    private final ByteBufferOutputStream bufferStream;
    private final byte magic;
    private final int initPos;
    private final long baseOffset;
    private final long logAppendTime;

    private long writtenUncompressed;
    private long numRecords;
    private float compressionRate;
    private long maxTimestamp;
    private long offsetOfMaxTimestamp;
    private long lastOffset = -1;

    public Compressor(ByteBuffer buffer,
                      byte magic,
                      CompressionType compressionType,
                      TimestampType timestampType,
                      long baseOffset,
                      long logAppendTime) {
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

        if (compressionType != CompressionType.NONE) {
            // for compressed records, leave space for the header and the shallow message metadata
            // and move the starting position to the value payload offset
            buffer.position(initPos + LogBuffer.LOG_OVERHEAD + Record.recordOverhead(magic));
        }

        // create the stream
        bufferStream = new ByteBufferOutputStream(buffer);
        appendStream = wrapForOutput(bufferStream, compressionType, magic, COMPRESSION_DEFAULT_BUFFER_SIZE);
    }

    public ByteBuffer buffer() {
        return bufferStream.buffer();
    }

    public double compressionRate() {
        return compressionRate;
    }

    public void close() {
        try {
            appendStream.close();
        } catch (IOException e) {
            throw new KafkaException(e);
        }

        if (compressionType != CompressionType.NONE) {
            ByteBuffer buffer = bufferStream.buffer();
            int pos = buffer.position();
            buffer.position(initPos);

            int wrapperSize = pos - LogBuffer.LOG_OVERHEAD;
            LogEntry.writeHeader(buffer, lastOffset, wrapperSize);

            long timestamp = timestampType == TimestampType.LOG_APPEND_TIME ? logAppendTime : maxTimestamp;
            Record.writeCompressedRecordHeader(buffer, magic, wrapperSize, timestamp, compressionType, timestampType);

            buffer.position(pos);

            // update the compression ratio
            this.compressionRate = (float) buffer.position() / this.writtenUncompressed;
            TYPE_TO_RATE[compressionType.id] = TYPE_TO_RATE[compressionType.id] * COMPRESSION_RATE_DAMPING_FACTOR +
                compressionRate * (1 - COMPRESSION_RATE_DAMPING_FACTOR);
        }
    }

    // Note that for all the write operations below, IO exceptions should
    // never be thrown since the underlying ByteBufferOutputStream does not throw IOException;
    // therefore upon encountering this issue we just close the append stream.

    public void putLong(final long value) {
        try {
            appendStream.writeLong(value);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    public void putInt(final int value) {
        try {
            appendStream.writeInt(value);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    public void put(final ByteBuffer buffer) {
        try {
            appendStream.write(buffer.array(), buffer.arrayOffset(), buffer.limit());
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Put a record as uncompressed into the underlying stream
     * @return CRC of the record
     */
    public long putRecord(long offset, long timestamp, byte[] key, byte[] value) {
        try {
            if (lastOffset > 0 && offset <= lastOffset)
                throw new IllegalArgumentException(String.format("Illegal offset %s following previous offset %s (Offsets must increase monotonically).", offset, lastOffset));

            int size = Record.recordSize(magic, key, value);
            LogEntry.writeHeader(this, toInnerOffset(offset), size);
            long crc = Record.write(appendStream, magic, timestamp, key, value, CompressionType.NONE, timestampType);
            recordWritten(offset, timestamp, size + LogBuffer.LOG_OVERHEAD);
            return crc;
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Add the record, converting to the desired magic value if necessary.
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void convertAndPutRecord(long offset, Record record) {
        if (magic == record.magic()) {
            putRecord(offset, record);
            return;
        }

        if (lastOffset > 0 && offset <= lastOffset)
            throw new IllegalArgumentException(String.format("Illegal offset %s following previous offset %s (Offsets must increase monotonically).", offset, lastOffset));

        try {
            int size = record.convertedSize(magic);
            LogEntry.writeHeader(this, toInnerOffset(offset), size);
            record.convertTo(appendStream, magic, logAppendTime, timestampType);
            recordWritten(offset, logAppendTime, size);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Add a record without doing offset/magic validation (this should only be used in testing).
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void putRecordUnchecked(long offset, Record record) {
        int size = record.size();
        LogEntry.writeHeader(this, toInnerOffset(offset), size);
        put(record.buffer().duplicate());
        recordWritten(offset, record.timestamp(), size);
    }

    /**
     * Add a record with a given offset. The record must have a magic which matches the magic use to
     * construct this Compressor instance.
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void putRecord(long offset, Record record) {
        if (record.magic() != magic)
            throw new IllegalArgumentException("Inner log entries must have matching magic values as the wrapper");
        if (lastOffset > 0 && offset <= lastOffset)
            throw new IllegalArgumentException(String.format("Illegal offset %s following previous offset %s (Offsets must increase monotonically).", offset, lastOffset));
        putRecordUnchecked(offset, record);
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
     * Get the maximum timestamp of all records added. If the log append time is used, then this
     * will return the timestamp used to create the Compressor itself.
     * @return The maximum timestamp
     */
    public long maxTimestamp() {
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            return logAppendTime;
        return maxTimestamp;
    }

    /**
     * Get the offset of the max timestamp. If the log append time is used, then this will return the
     * offset of the last record added.
     * @return The offset of the max timestamp
     */
    public long offsetOfMaxTimestamp() {
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            return lastOffset;
        return offsetOfMaxTimestamp;
    }

    /**
     * Get the number of records added to this collection.
     * @return The number of records added.
     */
    public long numRecordsWritten() {
        return numRecords;
    }

    /**
     * Get an estimate of the number of bytes written (based on the estimation factor hard-coded in {@link CompressionType}.
     * @return The estimated number of bytes written
     */
    public long estimatedBytesWritten() {
        if (compressionType == CompressionType.NONE) {
            return bufferStream.buffer().position();
        } else {
            // estimate the written bytes to the underlying byte buffer based on uncompressed written bytes
            return (long) (writtenUncompressed * TYPE_TO_RATE[compressionType.id] * COMPRESSION_RATE_ESTIMATION_FACTOR);
        }
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
                        Constructor constructor = delegate.get();
                        value = constructor;
                        initialized = true;
                        return constructor;
                    }
                }
            }
            return value;
        }
    }
}
