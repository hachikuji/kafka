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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsumerGroupController implements GroupController<PartitionAssignmentProtocol>, Metadata.MetadataListener {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupController.class);
    private static final String CONSUMER_GROUP_TYPE = "consumer";

    private final List<PartitionAssignor<?>> assignors;
    private final SubscriptionState subscription;
    private final Metadata metadata;
    private final Coordinator.RebalanceCallback rebalanceCallback;
    private Node coordinator;
    private Cluster cluster;

    public ConsumerGroupController(List<PartitionAssignor<?>> assignors,
                                   SubscriptionState subscription,
                                   Metadata metadata,
                                   Coordinator.RebalanceCallback rebalanceCallback) {
        this.assignors = assignors;
        this.subscription = subscription;
        this.metadata = metadata;
        this.rebalanceCallback = rebalanceCallback;
        this.metadata.addListener(this);
    }

    @Override
    public String groupType() {
        return CONSUMER_GROUP_TYPE;
    }

    @Override
    public List<PartitionAssignmentProtocol> protocols() {
        List<PartitionAssignmentProtocol> protocols = new ArrayList<>();
        MetadataSnapshot metadataSnapshot = metadataSnapshot(cluster);
        for (PartitionAssignor<?> assignor : assignors)
            protocols.add(new PartitionAssignmentProtocol(assignor, metadataSnapshot));
        return protocols;
    }

    @Override
    public void onCoordinatorFound(Node coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void onCoordinatorDead(Node coordinator) {
        this.coordinator = null;
    }

    @Override
    public void onJoin(PartitionAssignmentProtocol protocol, String memberId, Map<String, ByteBuffer> members) {
        @SuppressWarnings("unchecked")
        PartitionAssignor<Object> assignor = (PartitionAssignor<Object>) protocol.assignor();

        Type schema = assignor.schema();
        SortedMap<String, Object> memberMetadata = new TreeMap<>();
        for (Map.Entry<String, ByteBuffer> metadataEntry : members.entrySet()) {
            Object metadata = schema.read(metadataEntry.getValue());
            memberMetadata.put(metadataEntry.getKey(), metadata);
        }

        PartitionAssignor.AssignmentResult result = assignor.assign(memberId, memberMetadata, protocol.metadataSnapshot());
        if (!result.succeeded()) {
            // assignments fail due to conflicting metadata among group members, so we have to update our metadata
            subscription.groupSubscribe(result.groupSubscription());
            metadata.setTopics(result.groupSubscription());
            metadata.requestUpdate(coordinator);
            return;
        }

        subscription.changePartitionAssignment(result.assignment());

        // execute the user's callback after rebalance
        log.debug("Setting newly assigned partitions {}", subscription.assignedPartitions());
        try {
            Set<TopicPartition> assigned = new HashSet<>(subscription.assignedPartitions());
            rebalanceCallback.onPartitionsAssigned(assigned);
        } catch (Exception e) {
            log.error("User provided callback " + this.rebalanceCallback.getClass().getName()
                    + " failed on partition assignment: ", e);
        }
    }

    @Override
    public void onLeave(PartitionAssignmentProtocol protocol, String memberId, Map<String, ByteBuffer> members) {
        // execute the user's callback before rebalance
        log.debug("Revoking previously assigned partitions {}", subscription.assignedPartitions());
        try {
            Set<TopicPartition> revoked = new HashSet<>(subscription.assignedPartitions());
            rebalanceCallback.onPartitionsRevoked(revoked);
        } catch (Exception e) {
            log.error("User provided callback " + this.rebalanceCallback.getClass().getName()
                    + " failed on partition revocation: ", e);
        }
        subscription.needReassignment();
    }

    @Override
    public boolean needRejoin() {
        return subscription.partitionAssignmentNeeded();
    }

    private MetadataSnapshot metadataSnapshot(Cluster cluster) {
        Set<String> localSubscribedTopics = new HashSet<>(subscription.subscribedTopics());
        Set<String> groupSubscribedTopics = new HashSet<>(subscription.groupSubscribedTopics());

        SortedMap<String, MetadataSnapshot.TopicMetadata> topicMetadata = new TreeMap<>();
        for (String topic : union(localSubscribedTopics, groupSubscribedTopics)) {
            Integer partitions = cluster.partitionCountForTopic(topic);
            if (partitions != null)
                topicMetadata.put(topic, new MetadataSnapshot.TopicMetadata(partitions));
        }
        return new MetadataSnapshot(localSubscribedTopics, groupSubscribedTopics, topicMetadata);
    }

    private static <T> Set<T> union(Set<T> a, Set<T> b) {
        HashSet<T> res = new HashSet<>();
        res.addAll(a);
        res.addAll(b);
        return res;
    }

    @Override
    public void onMetadataUpdate(Cluster cluster) {
        // check if there are any changes to the metadata which should trigger a rebalance
        if (!cluster.equals(this.cluster)) {
            this.cluster = cluster;
            if (subscription.partitionsAutoAssigned())
                subscription.needReassignment();
        }
    }

}
