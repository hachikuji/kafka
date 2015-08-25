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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.MetadataSnapshot;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Type;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generic interface for client-side assignment implementations. Unless custom metadata is
 * needed, implementations will typically extend {@link org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor}.
 * @param <M> The type of the metadata
 */
public interface PartitionAssignor<M> {

    /**
     * Perform the assignment for the consumer.
     * @param consumerId The identifier of the local consumer instance
     * @param consumers Map of all consumers in the group and their respective metadata
     * @param metadataSnapshot The metadata snapshot that was used when the group was created, including
     *                         subscription information and topic metadata
     * @return
     */
    AssignmentResult assign(String consumerId,
                            Map<String, M> consumers,
                            MetadataSnapshot metadataSnapshot);

    /**
     * The name of the partition assignor (e.g. 'roundrobin')
     * @return The name (must not be null)
     */
    String name();

    /**
     * The version of the partition assignor
     * @return The version
     */
    short version();

    /**
     * Schema representing this assignor's metadata.
     * @return Non-null schema
     */
    Type schema();

    /**
     * Get the local metadata for this assignor. This will be invoked prior to every
     * round of group membership, so it is allowed to change dynamically. The metadata
     * will probably include at least the subscription of the consumer, but can also include
     * local information such as the number of processors on the host or its fqdn. Note,
     * however, that extra care must be taken when designing an assignor for large groups
     * since the metadata from each member must be propagated to ALL members of the group.
     *
     * @param metadataSnapshot Snapshot of the consumer's metadata/subscription.
     * @return The latest metadata for
     */
    M metadata(MetadataSnapshot metadataSnapshot);

    /**
     * Wrapper
     */
    class AssignmentResult {
        private boolean succeeded = false;
        private List<TopicPartition> assignment;
        private Set<String> groupSubscription;

        public AssignmentResult(boolean succeeded,
                                List<TopicPartition> assignment,
                                Set<String> groupSubscription) {
            this.succeeded = succeeded;
            this.assignment = assignment;
            this.groupSubscription = groupSubscription;
        }

        public boolean succeeded() {
            return succeeded;
        }

        public List<TopicPartition> assignment() {
            return assignment;
        }

        public Set<String> groupSubscription() {
            return groupSubscription;
        }

        /**
         * Successful assignment.
         * @param assignment The partitions assigned to the local group member
         * @return AssignmentResult indicated the success
         */
        public static AssignmentResult success(List<TopicPartition> assignment) {
            return new AssignmentResult(true, assignment, null);
        }

        /**
         * Failed assignment. Assignments can fail from inconsistent metadata. In that case, we need
         * to know the full list of subscriptions that were used by the group in order to
         * synchronize metadata.
         * @param groupSubscription Full set of subscriptions that group members are interested in.
         * @return AssignmentResult indicating the failure
         */
        public static AssignmentResult failure(Set<String> groupSubscription) {
            return new AssignmentResult(false, null, groupSubscription);
        }

    }



}
