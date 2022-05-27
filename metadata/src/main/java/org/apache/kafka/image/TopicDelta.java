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

package org.apache.kafka.image;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Represents changes to a topic in the metadata image.
 */
public final class TopicDelta {
    private final TopicImage image;
    private final Map<Integer, PartitionRegistration> partitionChanges = new HashMap<>();

    public TopicDelta(TopicImage image) {
        this.image = image;
    }

    public TopicImage image() {
        return image;
    }

    public Map<Integer, PartitionRegistration> partitionChanges() {
        return partitionChanges;
    }

    public String name() {
        return image.name();
    }

    public Uuid id() {
        return image.id();
    }

    public void replay(PartitionRecord record) {
        partitionChanges.put(record.partitionId(), new PartitionRegistration(record));
    }

    public void replay(PartitionChangeRecord record) {
        PartitionRegistration partition = partitionChanges.get(record.partitionId());
        if (partition == null) {
            partition = image.partitions().get(record.partitionId());
            if (partition == null) {
                throw new RuntimeException("Unable to find partition " +
                    record.topicId() + ":" + record.partitionId());
            }
        }
        partitionChanges.put(record.partitionId(), partition.merge(record));
    }

    public TopicImage apply() {
        Map<Integer, PartitionRegistration> newPartitions = new HashMap<>();
        for (Entry<Integer, PartitionRegistration> entry : image.partitions().entrySet()) {
            int partitionId = entry.getKey();
            PartitionRegistration changedPartition = partitionChanges.get(partitionId);
            if (changedPartition == null) {
                newPartitions.put(partitionId, entry.getValue());
            } else {
                newPartitions.put(partitionId, changedPartition);
            }
        }
        for (Entry<Integer, PartitionRegistration> entry : partitionChanges.entrySet()) {
            if (!newPartitions.containsKey(entry.getKey())) {
                newPartitions.put(entry.getKey(), entry.getValue());
            }
        }
        return new TopicImage(image.name(), image.id(), newPartitions);
    }

    /**
     * Find the partitions that have change based on the replica given.
     *
     * The changes identified are:
     *   1. partitions for which the broker is not a replica anymore
     *   2. partitions for which the broker is now the leader
     *   3. partitions for which the broker is now a follower
     *
     * @param brokerId the broker id
     * @return the list of partitions which the broker should remove, become leader or become follower.
     */
    public LocalReplicaChanges localChanges(int brokerId) {
        List<TopicIdPartition> deletes = new ArrayList<>();
        List<LocalReplicaChanges.PartitionInfo> leaders = new ArrayList<>();
        List<LocalReplicaChanges.PartitionInfo> followers = new ArrayList<>();

        for (Entry<Integer, PartitionRegistration> entry : partitionChanges.entrySet()) {
            PartitionRegistration newPartition = entry.getValue();
            Integer partitionId = entry.getKey();
            TopicPartition topicPartition = new TopicPartition(name(), partitionId);
            TopicIdPartition topicIdPartition = new TopicIdPartition(id(), topicPartition);

            if (!Replicas.contains(newPartition.replicas, brokerId)) {
                PartitionRegistration prevPartition = image.partitions().get(partitionId);
                if (prevPartition != null && Replicas.contains(prevPartition.replicas, brokerId)) {
                    deletes.add(topicIdPartition);
                }
            } else if (newPartition.leader == brokerId) {
                PartitionRegistration prevPartition = image.partitions().get(partitionId);
                if (prevPartition == null || prevPartition.partitionEpoch != newPartition.partitionEpoch) {
                    leaders.add(new LocalReplicaChanges.PartitionInfo(topicIdPartition, newPartition));
                }
            } else {
                PartitionRegistration prevPartition = image.partitions().get(partitionId);
                if (prevPartition == null || prevPartition.partitionEpoch != newPartition.partitionEpoch) {
                    followers.add(new LocalReplicaChanges.PartitionInfo(topicIdPartition, newPartition));
                }
            }
        }

        return new LocalReplicaChanges(deletes, leaders, followers);
    }

    @Override
    public String toString() {
        return "TopicDelta(" +
            "partitionChanges=" + partitionChanges +
            ')';
    }
}
