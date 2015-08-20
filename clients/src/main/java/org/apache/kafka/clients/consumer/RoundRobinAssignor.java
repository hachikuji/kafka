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
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * The roundrobin assignor lays out all the available partitions and all the available consumers. It
 * then proceeds to do a roundrobin assignment from partition to consumer. If the subscriptions of all consumer
 * instances are identical, then the partitions will be uniformly distributed. (i.e., the partition ownership counts
 * will be within a delta of exactly one across all consumers.)
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
 *
 * The assignment will be:
 * C0 -> [t0p0, t0p2, t1p1]
 * C1 -> [t0p1, t1p0, t1p2]
 */
public class RoundRobinAssignor extends AbstractPartitionAssignor {

    @Override
    public List<TopicPartition> assign(String consumerId,
                                       Map<String, Integer> partitionsPerTopic,
                                       Map<String, ConsumerMetadata> consumers) {
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        CircularIterator<String> assigner = new CircularIterator<>(sorted(consumers.keySet()));
        for (TopicPartition partition : allPartitions(partitionsPerTopic, consumers)) {
            final String topic = partition.topic();
            for (; !consumers.get(assigner.peek()).topics().contains(topic); assigner.next());
            put(assignment, assigner.next(), partition);
        }
        return assignment.get(consumerId);
    }

    private void put(Map<String, List<TopicPartition>> assignment,
                     String consumerId, TopicPartition partition) {
        List<TopicPartition> partitions = assignment.get(consumerId);
        if (partitions == null) {
            partitions = new ArrayList<>();
            assignment.put(consumerId, partitions);
        }
        partitions.add(partition);
    }

    public <T extends Comparable<T>> List<T> sorted(Collection<T> consumers) {
        List<T> res = new ArrayList<>(consumers);
        Collections.sort(res);
        return res;
    }

    public List<TopicPartition> allPartitions(Map<String, Integer> partitionsPerTopic,
                                              Map<String, ConsumerMetadata> consumerMetadata) {
        SortedSet<String> topics = new TreeSet<>();
        for (ConsumerMetadata metadata : consumerMetadata.values())
            topics.addAll(metadata.topics());

        List<TopicPartition> allPartitions = new ArrayList<>();
        for (String topic : topics) {
            Integer partitions = partitionsPerTopic.get(topic);
            for (int partition = 0; partition < partitions; partition++) {
                allPartitions.add(new TopicPartition(topic, partition));
            }
        }
        return allPartitions;
    }

    @Override
    public String name() {
        return "round-robin";
    }

    @Override
    public short version() {
        return 0;
    }

    private static class CircularIterator<T> implements Iterator<T> {
        int i = 0;
        private List<T> list;

        public CircularIterator(List<T> list) {
            if (list.isEmpty()) {
                throw new IllegalArgumentException("CircularIterator can only be used on non-empty lists");
            }
            this.list = list;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public T next() {
            T next = list.get(i);
            i = (i + 1) % list.size();
            return next;
        }

        public T peek() {
            return list.get(i);
        }

    }


}
