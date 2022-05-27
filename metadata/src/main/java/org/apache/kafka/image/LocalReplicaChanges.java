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
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;

import java.util.List;
import java.util.Set;
import java.util.Map;

public final class LocalReplicaChanges {
    private final List<TopicIdPartition> deletes;
    private final List<PartitionInfo> leaders;
    private final List<PartitionInfo> followers;

    LocalReplicaChanges(
        List<TopicIdPartition> deletes,
        List<PartitionInfo> leaders,
        List<PartitionInfo> followers
    ) {
        this.deletes = deletes;
        this.leaders = leaders;
        this.followers = followers;
    }

    public List<TopicIdPartition> deletes() {
        return deletes;
    }

    public List<PartitionInfo> leaders() {
        return leaders;
    }

    public List<PartitionInfo> followers() {
        return followers;
    }

    @Override
    public String toString() {
        return String.format(
            "LocalReplicaChanges(deletes = %s, leaders = %s, followers = %s)",
            deletes,
            leaders,
            followers
        );
    }

    public static final class PartitionInfo {
        private final TopicIdPartition topicIdPartition;
        private final PartitionRegistration partition;

        public PartitionInfo(TopicIdPartition topicIdPartition, PartitionRegistration partition) {
            this.topicIdPartition = topicIdPartition;
            this.partition = partition;
        }

        @Override
        public String toString() {
            return "PartitionInfo(" +
                "topicIdPartition=" + topicIdPartition +
                ", registration=" + partition +
                ')';
        }

        public Uuid topicId() {
            return topicIdPartition.topicId();
        }

        public String topic() {
            return topicIdPartition.topic();
        }

        public TopicPartition topicPartition() {
            return topicIdPartition.topicPartition();
        }

        public TopicIdPartition topicIdPartition() {
            return topicIdPartition;
        }

        public PartitionRegistration partition() {
            return partition;
        }

        public LeaderAndIsrPartitionState toLeaderAndIsrPartitionState(boolean isNew) {
            return new LeaderAndIsrPartitionState()
                .setTopicName(topicIdPartition.topic())
                .setPartitionIndex(topicIdPartition.partition())
                .setControllerEpoch(-1)
                .setLeader(partition.leader)
                .setLeaderEpoch(partition.leaderEpoch)
                .setIsr(Replicas.toList(partition.isr))
                .setPartitionEpoch(partition.partitionEpoch)
                .setReplicas(Replicas.toList(partition.replicas))
                .setAddingReplicas(Replicas.toList(partition.addingReplicas))
                .setRemovingReplicas(Replicas.toList(partition.removingReplicas))
                .setLeaderRecoveryState(partition.leaderRecoveryState.value())
                .setIsNew(isNew);
        }
    }
}
