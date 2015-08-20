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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.MetadataSnapshot;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RoundRobinAssignorTest {

    private ByteBuffer topicHash = ByteBuffer.wrap(new byte[] {});

    @Test
    public void matchingMetadata() {
        RoundRobinAssignor assignor = new RoundRobinAssignor();

        SortedMap<String, MetadataSnapshot.TopicMetadata> metadataA = new TreeMap<>();
        metadataA.put("foo", new MetadataSnapshot.TopicMetadata(5));
        metadataA.put("bar", new MetadataSnapshot.TopicMetadata(10));
        MetadataSnapshot snapshotA = new MetadataSnapshot(metadataA.keySet(), metadataA.keySet(), metadataA);


        SortedMap<String, MetadataSnapshot.TopicMetadata> metadataB = new TreeMap<>();
        metadataB.put("foo", new MetadataSnapshot.TopicMetadata(5));
        metadataB.put("bar", new MetadataSnapshot.TopicMetadata(10));
        MetadataSnapshot snapshotB = new MetadataSnapshot(metadataB.keySet(), metadataB.keySet(), metadataB);

        Map<String, AbstractPartitionAssignor.ConsumerMetadata> consumers = new HashMap<>();
        consumers.put("A", new AbstractPartitionAssignor.ConsumerMetadata(topics("foo", "bar"), ByteBuffer.wrap(snapshotA.hash())));
        consumers.put("B", new AbstractPartitionAssignor.ConsumerMetadata(topics("foo", "bar"), ByteBuffer.wrap(snapshotB.hash())));

        PartitionAssignor.AssignmentResult result = assignor.assign("A", consumers, snapshotA);
        assertTrue(result.succeeded());

        result = assignor.assign("B", consumers, snapshotB);
        assertTrue(result.succeeded());
    }

    @Test
    public void mismatchingMetadata() {
        RoundRobinAssignor assignor = new RoundRobinAssignor();

        SortedMap<String, MetadataSnapshot.TopicMetadata> metadataA = new TreeMap<>();
        metadataA.put("foo", new MetadataSnapshot.TopicMetadata(5));
        metadataA.put("bar", new MetadataSnapshot.TopicMetadata(10));
        MetadataSnapshot snapshotA = new MetadataSnapshot(metadataA.keySet(), metadataA.keySet(), metadataA);


        SortedMap<String, MetadataSnapshot.TopicMetadata> metadataB = new TreeMap<>();
        metadataB.put("foo", new MetadataSnapshot.TopicMetadata(6)); // disagreement over number of partitions
        metadataB.put("bar", new MetadataSnapshot.TopicMetadata(10));
        MetadataSnapshot snapshotB = new MetadataSnapshot(metadataB.keySet(), metadataB.keySet(), metadataB);

        Map<String, AbstractPartitionAssignor.ConsumerMetadata> consumers = new HashMap<>();
        consumers.put("A", new AbstractPartitionAssignor.ConsumerMetadata(topics("foo", "bar"), ByteBuffer.wrap(snapshotA.hash())));
        consumers.put("B", new AbstractPartitionAssignor.ConsumerMetadata(topics("foo", "bar"), ByteBuffer.wrap(snapshotB.hash())));

        PartitionAssignor.AssignmentResult result = assignor.assign("A", consumers, snapshotA);
        assertFalse(result.succeeded());

        result = assignor.assign("B", consumers, snapshotB);
        assertFalse(result.succeeded());
    }

    @Test
    public void assignMatchingSubscription() {
        RoundRobinAssignor assignor = new RoundRobinAssignor();

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put("foo", 3);
        partitionsPerTopic.put("bar", 3);

        Map<String, AbstractPartitionAssignor.ConsumerMetadata> consumers = new HashMap<>();
        consumers.put("A", new AbstractPartitionAssignor.ConsumerMetadata(topics("foo", "bar"), topicHash));
        consumers.put("B", new AbstractPartitionAssignor.ConsumerMetadata(topics("foo", "bar"), topicHash));

        assertEquals(Arrays.asList(tp("bar", 0), tp("bar", 2), tp("foo", 1)),
                assignor.assign("A", partitionsPerTopic, consumers));
        assertEquals(Arrays.asList(tp("bar", 1), tp("foo", 0), tp("foo", 2)),
                assignor.assign("B", partitionsPerTopic, consumers));
    }

    @Test
    public void assignNonmatchingSubscription() {
        RoundRobinAssignor assignor = new RoundRobinAssignor();

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put("foo", 3);
        partitionsPerTopic.put("bar", 3);

        Map<String, AbstractPartitionAssignor.ConsumerMetadata> consumers = new HashMap<>();
        consumers.put("A", new AbstractPartitionAssignor.ConsumerMetadata(topics("foo", "bar"), topicHash));
        consumers.put("B", new AbstractPartitionAssignor.ConsumerMetadata(topics("foo"), topicHash));

        assertEquals(Arrays.asList(tp("bar", 0), tp("bar", 1), tp("bar", 2), tp("foo", 1)),
                assignor.assign("A", partitionsPerTopic, consumers));
        assertEquals(Arrays.asList(tp("foo", 0), tp("foo", 2)),
                assignor.assign("B", partitionsPerTopic, consumers));
    }

    public static List<String> topics(String... topics) {
        return Arrays.asList(topics);
    }

    public static TopicPartition tp(String topic, int partition) {
        return new TopicPartition(topic, partition);
    }

}
