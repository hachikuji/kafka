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

import org.apache.kafka.clients.consumer.PartitionAssignor;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;

public class PartitionAssignmentProtocol implements GroupProtocol {
    private PartitionAssignor<?> assignor;
    private MetadataSnapshot metadataSnapshot;

    public PartitionAssignmentProtocol(PartitionAssignor<?> assignor, MetadataSnapshot metadataSnapshot) {
        this.assignor = assignor;
        this.metadataSnapshot = metadataSnapshot;
    }

    public PartitionAssignor<?> assignor() {
        return assignor;
    }

    public MetadataSnapshot metadataSnapshot() {
        return metadataSnapshot;
    }

    public String name() {
        return assignor.name();
    }

    public short version() {
        return assignor.version();
    }

    @Override
    public ByteBuffer metadata() {
        Type schema = assignor.schema();
        Object metadata = assignor.metadata(metadataSnapshot);
        ByteBuffer buf = ByteBuffer.allocate(schema.sizeOf(metadata));
        schema.write(buf, metadata);
        buf.flip();
        return buf;
    }

}
