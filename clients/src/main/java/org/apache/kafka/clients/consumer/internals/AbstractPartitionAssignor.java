/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.PartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractPartitionAssignor implements PartitionAssignor<AbstractPartitionAssignor.ConsumerMetadata> {
    private static final String SUBSCRIPTION_KEY_NAME = "subscription";
    private static final String TOPIC_KEY_NAME = "topics";
    private static final String TOPIC_PATTERN_KEY_NAME = "topic_pattern";
    private static final String METADATA_HASH_KEY_NAME = "metadata_hash";

    public static final Schema CONSUMER_METADATA_SUBSCRIPTION_V0 = new Schema(
            new Field(TOPIC_KEY_NAME, new ArrayOf(Type.STRING)),
            new Field(TOPIC_PATTERN_KEY_NAME, Type.STRING),
            new Field(METADATA_HASH_KEY_NAME, Type.BYTES));

    public static final Schema CONSUMER_METADATA_V0 = new Schema(
            new Field(SUBSCRIPTION_KEY_NAME, CONSUMER_METADATA_SUBSCRIPTION_V0));

    private final Logger log = LoggerFactory.getLogger(getClass());

    public abstract List<TopicPartition> assign(String consumerId,
                                                Map<String, Integer> partitionsPerTopic,
                                                Map<String, ConsumerMetadata> consumerMetadata);

    public Type schema() {
        return new Type() {
            @Override
            public void write(ByteBuffer buffer, Object o) {
                ConsumerMetadata metadata = (ConsumerMetadata) o;
                CONSUMER_METADATA_V0.write(buffer, metadata.struct);
            }

            @Override
            public Object read(ByteBuffer buffer) {
                Struct struct = (Struct) CONSUMER_METADATA_V0.read(buffer);
                return new ConsumerMetadata(struct);
            }

            @Override
            public ConsumerMetadata validate(Object o) {
                if (o instanceof ConsumerMetadata)
                    return (ConsumerMetadata) o;
                throw new SchemaException(o + " is not ConsumerMetadata.");
            }

            @Override
            public int sizeOf(Object o) {
                ConsumerMetadata metadata = (ConsumerMetadata) o;
                return CONSUMER_METADATA_V0.sizeOf(metadata.struct);
            }
        };
    }

    @Override
    public AssignmentResult assign(String consumerId,
                                   Map<String, ConsumerMetadata> consumers,
                                   MetadataSnapshot metadataSnapshot) {
        ByteBuffer topicMetadataHash = ByteBuffer.wrap(metadataSnapshot.hash());
        Set<String> allSubscribedTopics = new HashSet<>();
        boolean consistentMetadata = true;

        for (ConsumerMetadata metadata : consumers.values()) {
            consistentMetadata = consistentMetadata && topicMetadataHash.equals(metadata.topicMetadataHash);
            allSubscribedTopics.addAll(metadata.topics);
        }

        if (!consistentMetadata)
            return AssignmentResult.failure(allSubscribedTopics);

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (String topic : allSubscribedTopics) {
            MetadataSnapshot.TopicMetadata topicMetadata = metadataSnapshot.topicMetadata().get(topic);
            if (topicMetadata != null)
                partitionsPerTopic.put(topic, topicMetadata.numberPartitions());
            else
                log.debug("Skipping assignment for topic {} since no metadata is available", topic);
        }

        List<TopicPartition> assignment = assign(consumerId, partitionsPerTopic, consumers);
        return AssignmentResult.success(assignment);
    }

    @Override
    public ConsumerMetadata metadata(MetadataSnapshot metadata) {
        ByteBuffer metadataHash = ByteBuffer.wrap(metadata.hash());
        Set<String> subscription = metadata.localSubscibedTopics();

        Struct consumerMetadataStruct = new Struct(CONSUMER_METADATA_V0);
        Struct subscriptionStruct = consumerMetadataStruct.instance(SUBSCRIPTION_KEY_NAME);
        subscriptionStruct.set(TOPIC_KEY_NAME, new ArrayList<>(subscription).toArray());
        subscriptionStruct.set(TOPIC_PATTERN_KEY_NAME, "");
        subscriptionStruct.set(METADATA_HASH_KEY_NAME, metadataHash);
        consumerMetadataStruct.set(SUBSCRIPTION_KEY_NAME, subscriptionStruct);
        return new ConsumerMetadata(new ArrayList<>(subscription), metadataHash);
    }

    public static class ConsumerMetadata {
        private final ByteBuffer topicMetadataHash;
        private final List<String> topics;
        private final Struct struct;

        public ConsumerMetadata(List<String> topics, ByteBuffer topicMetadataHash) {
            this.topicMetadataHash = topicMetadataHash;
            this.topics = topics;
            this.struct = new Struct(CONSUMER_METADATA_V0);
            Struct subscription = this.struct.instance(SUBSCRIPTION_KEY_NAME);
            subscription.set(TOPIC_KEY_NAME, new ArrayList<>(topics).toArray());
            subscription.set(TOPIC_PATTERN_KEY_NAME, "");
            subscription.set(METADATA_HASH_KEY_NAME, topicMetadataHash);
            this.struct.set(SUBSCRIPTION_KEY_NAME, subscription);
        }

        public ConsumerMetadata(Struct struct) {
            this.struct = struct;
            Struct subscription = this.struct.getStruct(SUBSCRIPTION_KEY_NAME);
            this.topics = new ArrayList<>();
            Object[] topicsArray = subscription.getArray(TOPIC_KEY_NAME);
            for (Object topicObj : topicsArray) {
                String topic = (String) topicObj;
                this.topics.add(topic);
            }
            this.topicMetadataHash = subscription.getBytes(METADATA_HASH_KEY_NAME);
        }

        public List<String> topics() {
            return topics;
        }

    }

}
