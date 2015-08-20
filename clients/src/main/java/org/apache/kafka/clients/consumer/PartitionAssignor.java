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

public interface PartitionAssignor<M> {

    AssignmentResult assign(String consumerId,
                            Map<String, M> consumers,
                            MetadataSnapshot metadataSnapshot);

    String name();

    short version();

    Type schema();

    M metadata(MetadataSnapshot subscription);

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

        public static AssignmentResult success(List<TopicPartition> assignment) {
            return new AssignmentResult(true, assignment, null);
        }

        public static AssignmentResult failure(Set<String> groupSubscription) {
            return new AssignmentResult(false, null, groupSubscription);
        }

    }



}
