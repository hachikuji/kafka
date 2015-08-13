/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ConsumerMetadata {

    public static final String CONSUMER_GROUP_TYPE = "consumer";

    private static final String STRATEGY_METADATA_KEY_NAME = "strategy_metadata";
    private static final String SUBSCRIBED_TOPIC_KEY_NAME = "subscribed_topics";
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String NUMBER_PARTITIONS_KEY_NAME = "number_partitions";

    public static final Schema CONSUMER_METADATA_SUBSCRIPTION_V0 = new Schema(
            new Field(TOPIC_KEY_NAME, Type.STRING),
            new Field(NUMBER_PARTITIONS_KEY_NAME, Type.INT32));

    public static final Schema CONSUMER_METADATA_V0 = new Schema(
            new Field(SUBSCRIBED_TOPIC_KEY_NAME, new ArrayOf(CONSUMER_METADATA_SUBSCRIPTION_V0)),
            new Field(STRATEGY_METADATA_KEY_NAME, Type.BYTES));

    private Type assignmentMetadata;
    private final Struct struct;
    private final List<TopicSubscription> subscribedTopics;
    private final ByteBuffer strategyMetadata;

    public ConsumerMetadata(List<TopicSubscription> subscribedTopics,
                            ByteBuffer strategyMetadata) {
        this.struct = new Struct(CONSUMER_METADATA_V0);
        List<Struct> topicsArray = new ArrayList<>();
        for (TopicSubscription subscribedTopic : subscribedTopics) {
            Struct subscribedTopicData = struct.instance(SUBSCRIBED_TOPIC_KEY_NAME);
            subscribedTopicData.set(TOPIC_KEY_NAME, subscribedTopic.topic);
            subscribedTopicData.set(NUMBER_PARTITIONS_KEY_NAME, subscribedTopic.numberPartitions);
        }
        struct.set(SUBSCRIBED_TOPIC_KEY_NAME, topicsArray.toArray());
        struct.set(STRATEGY_METADATA_KEY_NAME, strategyMetadata);
        this.subscribedTopics = subscribedTopics;
        this.strategyMetadata = strategyMetadata;
    }

    public ConsumerMetadata(Struct struct) {
        this.struct = struct;
        this.strategyMetadata = struct.getBytes(STRATEGY_METADATA_KEY_NAME);
        this.subscribedTopics = new ArrayList<>();
        for (Object subscribedTopicObj : struct.getArray(SUBSCRIBED_TOPIC_KEY_NAME)) {
            Struct subscribedTopicData = (Struct) subscribedTopicObj;
            String topic = subscribedTopicData.getString(TOPIC_KEY_NAME);
            int numberPartitions = subscribedTopicData.getInt(NUMBER_PARTITIONS_KEY_NAME);
            subscribedTopics.add(new TopicSubscription(topic, numberPartitions));
        }
    }

    public ByteBuffer strategyMetadata() {
        return strategyMetadata;
    }

    public List<TopicSubscription> subscribedTopics() {
        return subscribedTopics;
    }

    public Struct toStruct() {
        return struct;
    }

    @Override
    public String toString() {
        return struct.toString();
    }

    @Override
    public int hashCode() {
        return struct.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ConsumerMetadata other = (ConsumerMetadata) obj;
        return struct.equals(other.struct);
    }

    public static class TopicSubscription {
        public final String topic;
        public final int numberPartitions;

        public TopicSubscription(String topic, int numberPartitions) {
            this.topic = topic;
            this.numberPartitions = numberPartitions;
        }
    }


}
