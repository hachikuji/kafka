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

import org.apache.kafka.common.utils.Utils;

import java.security.MessageDigest;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 * Represents a point-in-time view of the topic metadata corresponding to a
 * consumer's metadata.
 *
 * NOT thread-safe
 */
public class MetadataSnapshot {

    private final MessageDigest digest;
    private final Set<String> localSubscribedTopics;
    private final Set<String> groupSubscribedTopics;
    private final SortedMap<String, TopicMetadata> metadata;

    public MetadataSnapshot(
            Set<String> localSubscribedTopics,
            Set<String> groupSubscribedTopics,
            SortedMap<String, TopicMetadata> metadata) {
        this.localSubscribedTopics = localSubscribedTopics;
        this.groupSubscribedTopics = groupSubscribedTopics;
        this.metadata = metadata;

        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public SortedMap<String, TopicMetadata> topicMetadata() {
        return metadata;
    }

    public Set<String> localSubscibedTopics() {
        return localSubscribedTopics;
    }

    public Set<String> groupSubscribedTopics() {
        return groupSubscribedTopics;
    }

    public byte[] hash() {
        digest.reset();
        for (Map.Entry<String, TopicMetadata> topicEntry : metadata.entrySet()) {
            String topic = topicEntry.getKey();
            Integer partitions = topicEntry.getValue().numberPartitions;
            digest.update(Utils.utf8(topic));
            digest.update(Utils.toArrayLE(partitions));
        }
        return digest.digest();
    }

    public boolean contains(Set<String> topics) {
        return metadata.keySet().containsAll(topics);
    }

    public static class TopicMetadata {
        private int numberPartitions;

        public TopicMetadata(int numberPartitions) {
            this.numberPartitions = numberPartitions;
        }

        public int numberPartitions() {
            return numberPartitions;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetadataSnapshot that = (MetadataSnapshot) o;

        if (localSubscribedTopics != null ? !localSubscribedTopics.equals(that.localSubscribedTopics) : that.localSubscribedTopics != null)
            return false;
        if (groupSubscribedTopics != null ? !groupSubscribedTopics.equals(that.groupSubscribedTopics) : that.groupSubscribedTopics != null)
            return false;
        return !(metadata != null ? !metadata.equals(that.metadata) : that.metadata != null);

    }

    @Override
    public int hashCode() {
        int result = localSubscribedTopics != null ? localSubscribedTopics.hashCode() : 0;
        result = 31 * result + (groupSubscribedTopics != null ? groupSubscribedTopics.hashCode() : 0);
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        return result;
    }
}
