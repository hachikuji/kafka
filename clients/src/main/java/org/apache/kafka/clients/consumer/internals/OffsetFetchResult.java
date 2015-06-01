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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * Used to convey results from an offset fetch to the polling thread by way of a callback. In particular,
 * see the usage in {@link org.apache.kafka.clients.consumer.KafkaConsumer#fetchCommittedOffsets(Set, long)}.
 */
public class OffsetFetchResult {
    private Map<TopicPartition, Long> offsets;

    public void setOffsets(Map<TopicPartition, Long> offsets) {
        this.offsets = offsets;
    }

    public boolean isReady() {
        return offsets != null;
    }

    public Map<TopicPartition, Long> offsets() {
        return offsets;
    }

}
