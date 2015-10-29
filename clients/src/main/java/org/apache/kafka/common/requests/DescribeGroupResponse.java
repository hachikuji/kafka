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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescribeGroupResponse extends AbstractRequestResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.GROUP_METADATA.id);

    private static final String GROUPS_KEY_NAME = "groups";
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String GENERATION_ID_KEY_NAME = "generation_id";
    private static final String GROUP_STATE_KEY_NAME = "state";
    private static final String PROTOCOL_TYPE_KEY_NAME = "protocol_type";
    private static final String PROTOCOL_KEY_NAME = "protocol";

    public static final String UNKNOWN_STATE = "";
    public static final String UNKNOWN_PROTOCOL_TYPE = "";
    public static final String UNKNOWN_PROTOCOL = "";
    public static final int UNKNOWN_GENERATION = -1;

    private final Map<String, GroupMetadata> groups;

    public DescribeGroupResponse(Map<String, GroupMetadata> groups) {
        super(new Struct(CURRENT_SCHEMA));

        List<Struct> groupStructs = new ArrayList<>();
        for (Map.Entry<String, GroupMetadata> groupEntry : groups.entrySet()) {
            Struct groupStruct = struct.instance(GROUPS_KEY_NAME);
            groupStruct.set(GROUP_ID_KEY_NAME, groupEntry.getKey());

            GroupMetadata metadata = groupEntry.getValue();
            groupStruct.set(ERROR_CODE_KEY_NAME, metadata.errorCode);
            groupStruct.set(GROUP_STATE_KEY_NAME, metadata.state);
            groupStruct.set(GENERATION_ID_KEY_NAME, metadata.generationId);
            groupStruct.set(PROTOCOL_TYPE_KEY_NAME, metadata.protocolType);
            groupStruct.set(PROTOCOL_KEY_NAME, metadata.protocol);
            groupStructs.add(groupStruct);
        }
        struct.set(GROUPS_KEY_NAME, groupStructs.toArray());
        this.groups = groups;
    }

    public DescribeGroupResponse(Struct struct) {
        super(struct);

        this.groups = new HashMap<>();
        Object[] groupStructs = struct.getArray(GROUPS_KEY_NAME);
        for (Object groupObj : groupStructs) {
            Struct group = (Struct) groupObj;
            String groupId = group.getString(GROUP_ID_KEY_NAME);
            short errorCode = group.getShort(ERROR_CODE_KEY_NAME);


            String state = group.getString(GROUP_STATE_KEY_NAME);
            int generation = group.getInt(GENERATION_ID_KEY_NAME);
            String protocolType = group.getString(PROTOCOL_TYPE_KEY_NAME);
            String protocol = group.getString(PROTOCOL_KEY_NAME);

            this.groups.put(groupId, new GroupMetadata(errorCode, state, generation,
                    protocolType, protocol));
        }
    }

    public static DescribeGroupResponse parse(ByteBuffer buffer) {
        return new DescribeGroupResponse((Struct) CURRENT_SCHEMA.read(buffer));
    }


    public static class GroupMetadata {
        private final short errorCode;
        private final String state;
        private final int generationId;
        private final String protocolType;
        private final String protocol;

        public GroupMetadata(short errorCode,
                             String state,
                             int generationId,
                             String protocolType,
                             String protocol) {
            this.errorCode = errorCode;
            this.state = state;
            this.generationId = generationId;
            this.protocolType = protocolType;
            this.protocol = protocol;
        }

        public short errorCode() {
            return errorCode;
        }

        public String state() {
            return state;
        }

        public int generationId() {
            return generationId;
        }

        public String protocolType() {
            return protocolType;
        }

        public String protocol() {
            return protocol;
        }

    }
}
