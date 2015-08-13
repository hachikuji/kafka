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

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RoundRobinAssignorTest {

    @Test
    public void assignMatchingSubscription() {
        RoundRobinAssignor assignor = new RoundRobinAssignor();

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put("foo", 3);
        partitionsPerTopic.put("bar", 3);

        List<PartitionAssignor.ConsumerMetadata<Void>> consumers = Arrays.asList(
                new PartitionAssignor.ConsumerMetadata<Void>("A", null, topics("foo", "bar")),
                new PartitionAssignor.ConsumerMetadata<Void>("B", null, topics("foo", "bar"))
        );

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

        List<PartitionAssignor.ConsumerMetadata<Void>> consumers = Arrays.asList(
                new PartitionAssignor.ConsumerMetadata<Void>("A", null, topics("foo", "bar")),
                new PartitionAssignor.ConsumerMetadata<Void>("B", null, topics("foo"))
        );

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
