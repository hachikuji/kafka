/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;

/**
 * High-level representation of a kafka record. This is useful when building record sets to
 * avoid depending on a specific magic version.
 */
public class KafkaRecord {

    private final ByteBuffer key;
    private final ByteBuffer value;
    private final long timestamp;

    public KafkaRecord(long timestamp, ByteBuffer key, ByteBuffer value) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public KafkaRecord(long timestamp, byte[] key, byte[] value) {
        this(timestamp, Utils.wrapNullable(key), Utils.wrapNullable(value));
    }

    public KafkaRecord(long timestamp, byte[] value) {
        this(timestamp, null, value);
    }

    public KafkaRecord(byte[] value) {
        this(LogEntry.NO_TIMESTAMP, null, value);
    }

    public KafkaRecord(byte[] key, byte[] value) {
        this(LogEntry.NO_TIMESTAMP, key, value);
    }

    public KafkaRecord(LogRecord logRecord) {
        this(logRecord.timestamp(), logRecord.key(), logRecord.value());
    }

    public ByteBuffer key() {
        return key;
    }

    public ByteBuffer value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KafkaRecord that = (KafkaRecord) o;

        if (timestamp != that.timestamp) return false;
        if (key != null ? !key.equals(that.key) : that.key != null) return false;
        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

}
