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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.MetadataSnapshot;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;
import java.util.Map;

public class MockPartitionAssignor implements PartitionAssignor<Void> {

    private AssignmentResult result = null;

    @Override
    public PartitionAssignor.AssignmentResult assign(String consumerId, Map<String, Void> consumers, MetadataSnapshot metadataSnapshot) {
        if (result == null)
            throw new IllegalStateException("Call to assign with no result prepared");
        return result;
    }

    @Override
    public String name() {
        return "mock-assignor";
    }

    @Override
    public short version() {
        return 0;
    }

    public void clear() {
        this.result = null;
    }

    public void prepare(AssignmentResult result) {
        this.result = result;
    }

    @Override
    public Type schema() {
        return new Type() {
            @Override
            public void write(ByteBuffer buffer, Object o) {
            }

            @Override
            public Object read(ByteBuffer buffer) {
                return null;
            }

            @Override
            public Object validate(Object o) {
                return null;
            }

            @Override
            public int sizeOf(Object o) {
                return 0;
            }
        };
    }

    @Override
    public Void metadata(MetadataSnapshot metadataSnapshot) {
        return null;
    }
}
