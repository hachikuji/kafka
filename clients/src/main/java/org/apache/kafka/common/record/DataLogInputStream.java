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

import org.apache.kafka.common.errors.CorruptRecordException;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Simple log input stream which is backed by an underlying {@link DataInputStream}.
 */
class DataLogInputStream implements LogInputStream<LogEntry> {
    private final DataInputStream stream;
    protected final int maxMessageSize;

    DataLogInputStream(DataInputStream stream, int maxMessageSize) {
        this.stream = stream;
        this.maxMessageSize = maxMessageSize;
    }

    public LogEntry nextEntry() throws IOException {
        long offset = stream.readLong();
        int size = stream.readInt();
        if (size < Record.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Record size is less than the minimum record overhead (%d)", Record.RECORD_OVERHEAD_V0));
        if (size > maxMessageSize)
            throw new CorruptRecordException(String.format("Record size exceeds the largest allowable message size (%d).", maxMessageSize));

        byte[] recordBuffer = new byte[size];
        stream.readFully(recordBuffer, 0, size);
        ByteBuffer buf = ByteBuffer.wrap(recordBuffer);
        return new LogEntry.SimpleLogEntry(offset, new Record(buf));
    }
}
