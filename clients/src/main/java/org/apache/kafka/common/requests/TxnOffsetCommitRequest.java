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
package org.apache.kafka.common.requests;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TxnOffsetCommitRequest extends AbstractRequest {
    private static final String CONSUMER_GROUP_ID_KEY_NAME = "consumer_group_id";
    private static final String PID_KEY_NAME = "pid";
    private static final String EPOCH_KEY_NAME = "epoch";
    private static final String RETENTION_TIME_KEY_NAME = "retention_time";
    private static final String TOPIC_PARTITIONS_KEY_NAME = "topic_partitions";
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String OFFSET_KEY_NAME = "offset";
    private static final String METADATA_KEY_NAME = "metadata";

    public static class Builder extends AbstractRequest.Builder<TxnOffsetCommitRequest> {
        private final String consumerGroupId;
        private final long pid;
        private final short epoch;
        private final long retentionTimeMs;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        public Builder(String consumerGroupId, long pid, short epoch, long retentionTimeMs,
                       Map<TopicPartition, OffsetAndMetadata> offsets) {
            super(ApiKeys.TXN_OFFSET_COMMIT);
            this.consumerGroupId = consumerGroupId;
            this.pid = pid;
            this.epoch = epoch;
            this.retentionTimeMs = retentionTimeMs;
            this.offsets = offsets;
        }

        @Override
        public TxnOffsetCommitRequest build(short version) {
            return new TxnOffsetCommitRequest(version, consumerGroupId, pid, epoch, retentionTimeMs, offsets);
        }
    }

    private final String consumerGroupId;
    private final long pid;
    private final short epoch;
    private final long retentionTimeMs;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    public TxnOffsetCommitRequest(short version , String consumerGroupId, long pid, short epoch,
                                  long retentionTimeMs, Map<TopicPartition, OffsetAndMetadata> offsets) {
        super(version);
        this.consumerGroupId = consumerGroupId;
        this.pid = pid;
        this.epoch = epoch;
        this.retentionTimeMs = retentionTimeMs;
        this.offsets = offsets;
    }

    public TxnOffsetCommitRequest(Struct struct, short version) {
        super(version);
        this.consumerGroupId = struct.getString(CONSUMER_GROUP_ID_KEY_NAME);
        this.pid = struct.getLong(PID_KEY_NAME);
        this.epoch = struct.getShort(EPOCH_KEY_NAME);
        this.retentionTimeMs = struct.getLong(RETENTION_TIME_KEY_NAME);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        Object[] topicPartitionsArray = struct.getArray(TOPIC_PARTITIONS_KEY_NAME);
        for (Object topicPartitionObj : topicPartitionsArray) {
            Struct topicPartitionStruct = (Struct) topicPartitionObj;
            String topic = topicPartitionStruct.getString(TOPIC_KEY_NAME);
            for (Object partitionObj : topicPartitionStruct.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionStruct = (Struct) partitionObj;
                TopicPartition partition = new TopicPartition(topic, partitionStruct.getInt(PARTITION_KEY_NAME));
                long offset = partitionStruct.getLong(OFFSET_KEY_NAME);
                String metadata = partitionStruct.getString(METADATA_KEY_NAME);
                offsets.put(partition, new OffsetAndMetadata(offset, metadata));
            }
        }
        this.offsets = offsets;
    }

    public String consumerGroupId() {
        return consumerGroupId;
    }

    public long pid() {
        return pid;
    }

    public short epoch() {
        return epoch;
    }

    public long retentionTimeMs() {
        return retentionTimeMs;
    }

    public Map<TopicPartition, OffsetAndMetadata> offsets() {
        return offsets;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.TXN_OFFSET_COMMIT.requestSchema(version()));
        struct.set(CONSUMER_GROUP_ID_KEY_NAME, consumerGroupId);
        struct.set(PID_KEY_NAME, pid);
        struct.set(EPOCH_KEY_NAME, epoch);
        struct.set(RETENTION_TIME_KEY_NAME, retentionTimeMs);

        Map<String, List<PartitionAndOffset>> partitionOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> offsetEntry : offsets.entrySet()) {
            TopicPartition topicPartition = offsetEntry.getKey();
            List<PartitionAndOffset> partitionOffsetList = partitionOffsets.get(topicPartition.topic());
            if (partitionOffsetList == null) {
                partitionOffsetList = new ArrayList<>();
                partitionOffsets.put(topicPartition.topic(), partitionOffsetList);
            }
            partitionOffsetList.add(new PartitionAndOffset(topicPartition.partition(), offsetEntry.getValue()));
        }

        Object[] partitionsArray = new Object[partitionOffsets.size()];
        int i = 0;
        for (Map.Entry<String, List<PartitionAndOffset>> topicAndPartitions : partitionOffsets.entrySet()) {
            Struct topicPartitionsStruct = struct.instance(TOPIC_PARTITIONS_KEY_NAME);
            topicPartitionsStruct.set(TOPIC_KEY_NAME, topicAndPartitions.getKey());

            List<PartitionAndOffset> partitionOffsetList = topicAndPartitions.getValue();
            Object[] partitionOffsetsArray = new Object[partitionOffsetList.size()];
            int j = 0;
            for (PartitionAndOffset partitionOffset: partitionOffsetList) {
                Struct partitionOffsetStruct = topicPartitionsStruct.instance(PARTITIONS_KEY_NAME);
                partitionOffsetStruct.set(PARTITION_KEY_NAME, partitionOffset.partition);
                partitionOffsetStruct.set(OFFSET_KEY_NAME, partitionOffset.offset.offset());
                partitionOffsetStruct.set(METADATA_KEY_NAME, partitionOffset.offset.metadata());
                partitionOffsetsArray[j++] = partitionOffsetStruct;
            }
            topicPartitionsStruct.set(PARTITIONS_KEY_NAME, partitionOffsetsArray);
            partitionsArray[i++] = topicPartitionsStruct;
        }

        struct.set(TOPIC_PARTITIONS_KEY_NAME, partitionsArray);
        return struct;
    }

    @Override
    public TxnOffsetCommitResponse getErrorResponse(Throwable e) {
        Errors error = Errors.forException(e);
        Map<TopicPartition, Errors> errors = new HashMap<>(offsets.size());
        for (TopicPartition partition : offsets.keySet())
            errors.put(partition, error);
        return new TxnOffsetCommitResponse(errors);
    }

    public static TxnOffsetCommitRequest parse(ByteBuffer buffer, short version) {
        return new TxnOffsetCommitRequest(ApiKeys.TXN_OFFSET_COMMIT.parseRequest(version, buffer), version);
    }

    private static class PartitionAndOffset {
        private final int partition;
        private final OffsetAndMetadata offset;

        public PartitionAndOffset(int partition, OffsetAndMetadata offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }

}
