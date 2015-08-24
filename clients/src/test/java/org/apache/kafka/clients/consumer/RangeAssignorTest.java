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
 */
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RangeAssignorTest {

    private ByteBuffer topicHash = ByteBuffer.wrap(new byte[] {});

    @Test
    public void testOneConsumerNoTopic() {
        String consumerId = "consumer";
        RangeAssignor assignor = new RangeAssignor();
        AbstractPartitionAssignor.ConsumerMetadata metadata = new AbstractPartitionAssignor.ConsumerMetadata(
                Collections.<String>emptyList(), topicHash);
        Map<String, Integer> partitionsPerTopic = new HashMap<>();

        List<TopicPartition> assignment = assignor.assign(consumerId, partitionsPerTopic,
                Collections.singletonMap(consumerId, metadata));
        assertTrue(assignment.isEmpty());
    }

    @Test
    public void testOneConsumerNonexistentTopic() {
        String topic = "topic";
        String consumerId = "consumer";
        RangeAssignor assignor = new RangeAssignor();
        AbstractPartitionAssignor.ConsumerMetadata metadata = new AbstractPartitionAssignor.ConsumerMetadata(
                Arrays.asList(topic), topicHash);
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 0);

        List<TopicPartition> assignment = assignor.assign(consumerId, partitionsPerTopic,
                Collections.singletonMap(consumerId, metadata));
        assertTrue(assignment.isEmpty());
    }

    @Test
    public void testOneConsumerOneTopic() {
        String topic = "topic";
        String consumerId = "consumer";
        RangeAssignor assignor = new RangeAssignor();
        AbstractPartitionAssignor.ConsumerMetadata metadata = new AbstractPartitionAssignor.ConsumerMetadata(
                Arrays.asList(topic), topicHash);
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);

        List<TopicPartition> assignment = assignor.assign(consumerId, partitionsPerTopic,
                Collections.singletonMap(consumerId, metadata));
        assertEquals(Arrays.asList(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2)), assignment);
    }

    @Test
    public void testOnlyAssignsPartitionsFromSubscribedTopics() {
        String topic = "topic";
        String otherTopic = "other";
        String consumerId = "consumer";
        RangeAssignor assignor = new RangeAssignor();
        AbstractPartitionAssignor.ConsumerMetadata metadata = new AbstractPartitionAssignor.ConsumerMetadata(
                Arrays.asList(topic), topicHash);
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        partitionsPerTopic.put(otherTopic, 3);

        List<TopicPartition> assignment = assignor.assign(consumerId, partitionsPerTopic,
                Collections.singletonMap(consumerId, metadata));
        assertEquals(Arrays.asList(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2)), assignment);
    }

    @Test
    public void testOneConsumerMultipleTopics() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumerId = "consumer";
        RangeAssignor assignor = new RangeAssignor();
        AbstractPartitionAssignor.ConsumerMetadata metadata = new AbstractPartitionAssignor.ConsumerMetadata(
                Arrays.asList(topic1, topic2), topicHash);
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 1);
        partitionsPerTopic.put(topic2, 2);

        List<TopicPartition> assignment = assignor.assign(consumerId, partitionsPerTopic,
                Collections.singletonMap(consumerId, metadata));
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 0),
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 1)), assignment);
    }

    @Test
    public void testTwoConsumersOneTopicOnePartition() {
        String topic = "topic";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        RangeAssignor assignor = new RangeAssignor();
        AbstractPartitionAssignor.ConsumerMetadata metadata1 = new AbstractPartitionAssignor.ConsumerMetadata(
                Arrays.asList(topic), topicHash);
        AbstractPartitionAssignor.ConsumerMetadata metadata2 = new AbstractPartitionAssignor.ConsumerMetadata(
                Arrays.asList(topic), topicHash);

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 1);

        Map<String, AbstractPartitionAssignor.ConsumerMetadata> consumers = new HashMap<>();
        consumers.put(consumer1, metadata1);
        consumers.put(consumer2, metadata2);

        List<TopicPartition> assignment = assignor.assign(consumer1, partitionsPerTopic, consumers);
        assertEquals(Arrays.asList(new TopicPartition(topic, 0)), assignment);

        assignment = assignor.assign(consumer2, partitionsPerTopic, consumers);
        assertEquals(Collections.<TopicPartition>emptyList(), assignment);
    }


    @Test
    public void testTwoConsumersOneTopicTwoPartitions() {
        String topic = "topic";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        RangeAssignor assignor = new RangeAssignor();
        AbstractPartitionAssignor.ConsumerMetadata metadata1 = new AbstractPartitionAssignor.ConsumerMetadata(
                Arrays.asList(topic), topicHash);
        AbstractPartitionAssignor.ConsumerMetadata metadata2 = new AbstractPartitionAssignor.ConsumerMetadata(
                Arrays.asList(topic), topicHash);

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 2);

        Map<String, AbstractPartitionAssignor.ConsumerMetadata> consumers = new HashMap<>();
        consumers.put(consumer1, metadata1);
        consumers.put(consumer2, metadata2);

        List<TopicPartition> assignment = assignor.assign(consumer1, partitionsPerTopic, consumers);
        assertEquals(Arrays.asList(new TopicPartition(topic, 0)), assignment);

        assignment = assignor.assign(consumer2, partitionsPerTopic, consumers);
        assertEquals(Arrays.asList(new TopicPartition(topic, 1)), assignment);
    }

    @Test
    public void testMultipleConsumersMixedTopics() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        String consumer3 = "consumer3";
        RangeAssignor assignor = new RangeAssignor();
        AbstractPartitionAssignor.ConsumerMetadata metadata1 = new AbstractPartitionAssignor.ConsumerMetadata(
                Arrays.asList(topic1), topicHash);
        AbstractPartitionAssignor.ConsumerMetadata metadata2 = new AbstractPartitionAssignor.ConsumerMetadata(
                Arrays.asList(topic1, topic2), topicHash);
        AbstractPartitionAssignor.ConsumerMetadata metadata3 = new AbstractPartitionAssignor.ConsumerMetadata(
                Arrays.asList(topic1), topicHash);

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 2);

        Map<String, AbstractPartitionAssignor.ConsumerMetadata> consumers = new HashMap<>();
        consumers.put(consumer1, metadata1);
        consumers.put(consumer2, metadata2);
        consumers.put(consumer3, metadata3);

        List<TopicPartition> assignment = assignor.assign(consumer1, partitionsPerTopic, consumers);
        assertEquals(Arrays.asList(new TopicPartition(topic1, 0)), assignment);

        assignment = assignor.assign(consumer2, partitionsPerTopic, consumers);
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 1),
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 1)), assignment);

        assignment = assignor.assign(consumer3, partitionsPerTopic, consumers);
        assertEquals(Arrays.asList(new TopicPartition(topic1, 2)), assignment);
    }

    @Test
    public void testTwoConsumersTwoTopicsSixPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        RangeAssignor assignor = new RangeAssignor();
        AbstractPartitionAssignor.ConsumerMetadata metadata1 = new AbstractPartitionAssignor.ConsumerMetadata(
                Arrays.asList(topic1, topic2), topicHash);
        AbstractPartitionAssignor.ConsumerMetadata metadata2 = new AbstractPartitionAssignor.ConsumerMetadata(
                Arrays.asList(topic1, topic2), topicHash);

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 3);

        Map<String, AbstractPartitionAssignor.ConsumerMetadata> consumers = new HashMap<>();
        consumers.put(consumer1, metadata1);
        consumers.put(consumer2, metadata2);

        List<TopicPartition> assignment = assignor.assign(consumer1, partitionsPerTopic, consumers);
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 0),
                new TopicPartition(topic1, 1),
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 1)), assignment);

        assignment = assignor.assign(consumer2, partitionsPerTopic, consumers);
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 2),
                new TopicPartition(topic2, 2)), assignment);
    }

}
