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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.MockPartitionAssignor;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConsumerGroupControllerTest {

    private String topicName = "topic";
    private MockTime time;
    private Cluster cluster = TestUtils.singletonCluster(topicName, 2);
    private SubscriptionState subscriptions;
    private Metadata metadata;

    @Before
    public void setup() {
        this.time = new MockTime();
        this.subscriptions = new SubscriptionState(OffsetResetStrategy.EARLIEST);
        this.metadata = new Metadata(0, Long.MAX_VALUE);
        this.metadata.update(cluster, time.milliseconds());
    }

    @Test
    public void assignmentSuccess() {
        subscriptions.subscribe(topicName);
        MockPartitionAssignor assignor = new MockPartitionAssignor();
        MockRebalanceCallback callback = new MockRebalanceCallback();
        ConsumerGroupController controller = new ConsumerGroupController(
                Arrays.<PartitionAssignor<?>>asList(assignor), subscriptions, metadata, callback);

        List<PartitionAssignmentProtocol> protocols = controller.protocols();
        assertEquals(1, protocols.size());

        List<TopicPartition> assignment = Arrays.asList(new TopicPartition(topicName, 0));
        assignor.prepare(PartitionAssignor.AssignmentResult.success(assignment));
        controller.onJoin(protocols.get(0), "consumer", Collections.singletonMap("consumer", protocols.get(0).metadata()));

        assertFalse(subscriptions.partitionAssignmentNeeded());
        assertEquals(new HashSet<>(assignment), subscriptions.assignedPartitions());
        assertEquals(1, callback.assignInvocations);
    }

    @Test
    public void assignmentFailure() {
        subscriptions.subscribe(topicName);
        MockPartitionAssignor assignor = new MockPartitionAssignor();
        MockRebalanceCallback callback = new MockRebalanceCallback();
        ConsumerGroupController controller = new ConsumerGroupController(
                Arrays.<PartitionAssignor<?>>asList(assignor), subscriptions, metadata, callback);

        List<PartitionAssignmentProtocol> protocols = controller.protocols();
        assertEquals(1, protocols.size());

        Set<String> groupSubscribedTopics = new HashSet<>();
        groupSubscribedTopics.add(topicName);
        groupSubscribedTopics.add("otherTopic");

        assignor.prepare(PartitionAssignor.AssignmentResult.failure(groupSubscribedTopics));
        controller.onJoin(protocols.get(0), "consumer", Collections.singletonMap("consumer", protocols.get(0).metadata()));

        assertTrue(subscriptions.partitionAssignmentNeeded());
        assertEquals(0, callback.assignInvocations);
        assertEquals(groupSubscribedTopics, metadata.topics());
        assertTrue(metadata.timeToNextUpdate(time.milliseconds()) <= 0);
    }


    private static class MockRebalanceCallback implements Coordinator.RebalanceCallback {
        private int assignInvocations = 0;
        private int revokeInvocations = 0;
        private Collection<TopicPartition> assigned;
        private Collection<TopicPartition> revoked;

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            this.assigned = partitions;
            assignInvocations++;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            this.revoked = partitions;
            revokeInvocations++;
        }
    }


}
