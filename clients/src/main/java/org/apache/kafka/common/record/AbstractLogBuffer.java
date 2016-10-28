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

import org.apache.kafka.common.utils.AbstractIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractLogBuffer implements LogBuffer {

    @Override
    public boolean hasMatchingShallowMagic(byte magic) {
        Iterator<LogEntry> iterator = iterator(true);
        while (iterator.hasNext())
            if (iterator.next().record().magic() != magic)
                return false;
        return true;
    }

    /**
     * Convert this message set to use the specified message format.
     */
    @Override
    public LogBuffer toMessageFormat(byte toMagic) {
        List<LogEntry> converted = new ArrayList<>();
        Iterator<LogEntry> deepIterator = iterator(false);
        while (deepIterator.hasNext()) {
            LogEntry entry = deepIterator.next();
            converted.add(LogEntry.create(entry.offset(), entry.record().convert(toMagic)));
        }

        if (converted.isEmpty()) {
            // This indicates that the message is too large. We just return all the bytes in the file message set.
            // TODO: Is this really what we want? The consumer is still going to break if it gets the wrong version
            // would it be better to just return an empty buffer instead?
            return this;
        } else {
            // We use the offset seq to assign offsets so the offset of the messages does not change.
            // TODO: Using the compression like this seems wrong. Also, there's no guarantee that timestamp
            // types match, so putting all the messages into a wrapped message would cause us to lose log append time.
            // Maybe this is ok since we only use this for down-conversion in practice?
            CompressionType compressionType = iterator(true).next().record().compressionType();
            return MemoryLogBuffer.withLogEntries(compressionType, converted);
        }
    }

    public static int estimatedSize(CompressionType compressionType, Iterable<LogEntry> entries) {
        int size = 0;
        for (LogEntry entry : entries)
            size += entry.size();
        return compressionType == CompressionType.NONE ? size : Math.min(Math.max(size / 2, 1024), 1 << 16);
    }

    public Iterator<Record> records(final boolean isShallow) {
        return new AbstractIterator<Record>() {
            private final Iterator<LogEntry> entries = iterator(isShallow);
            @Override
            protected Record makeNext() {
                if (entries.hasNext())
                    return entries.next().record();
                return allDone();
            }
        };
    }

}
