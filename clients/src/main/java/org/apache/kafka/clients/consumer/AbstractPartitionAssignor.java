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

import org.apache.kafka.clients.consumer.internals.GroupProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public abstract class AbstractPartitionAssignor<M, S extends Type> implements GroupProtocol, PartitionAssignor<M, S> {


    @Override
    public List<TopicPartition> assign(String consumerId, Map<String, M> consumers) {
        return null;
    }

    @Override
    public S schema() {
        return null;
    }

    @Override
    public ByteBuffer metadata() {
        return null;
    }

    @Override
    public void onJoin(String memberId, Map<String, ByteBuffer> members) {

    }

    class ConsumerMetadata<M> implements Comparable<ConsumerMetadata<M>>{
        private final String consumerId;
        private final M metadata;
        private final List<String> subscribedTopics;

        public ConsumerMetadata(String consumerId,
                                M metadata,
                                List<String> subscribedTopics) {
            this.consumerId = consumerId;
            this.metadata = metadata;
            this.subscribedTopics = subscribedTopics;
        }

        public String consumerId() {
            return consumerId;
        }

        public T metadata() {
            return metadata;
        }

        public List<String> subscribedTopics() {
            return subscribedTopics;
        }

        @Override
        public int compareTo(ConsumerMetadata<T> o) {
            return consumerId.compareTo(o.consumerId);
        }
    }
}
