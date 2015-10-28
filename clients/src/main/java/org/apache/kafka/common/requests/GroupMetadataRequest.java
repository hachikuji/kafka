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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupMetadataRequest extends AbstractRequest {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.GROUP_METADATA.id);
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String GROUP_IDS_KEY_NAME = "group_ids";
    private static final String OPTIONS_KEY_NAME = "options";

    private final List<String> groupIds;
    private final boolean includeAllGroups;

    /**
     * Version 0 constructor
     * @param groupId
     */
    public GroupMetadataRequest(String groupId) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.GROUP_METADATA.id, 0)));
        struct.set(GROUP_ID_KEY_NAME, groupId);
        this.groupIds = Collections.singletonList(groupId);
        this.includeAllGroups = false;
    }

    /**
     * Version 1 constructor
     * @param groupIds
     * @param includeAllGroups
     */
    public GroupMetadataRequest(List<String> groupIds,
                                boolean includeAllGroups) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(OPTIONS_KEY_NAME, (byte) (includeAllGroups ? 1 : 0));
        struct.set(GROUP_IDS_KEY_NAME, groupIds.toArray());
        this.groupIds = groupIds;
        this.includeAllGroups = includeAllGroups;
    }

    public GroupMetadataRequest(Struct struct) {
        super(struct);

        if (struct.hasField(OPTIONS_KEY_NAME))
            this.includeAllGroups = (struct.getByte(OPTIONS_KEY_NAME) & 1) == 1;
        else
            this.includeAllGroups = false;

        if (struct.hasField(GROUP_ID_KEY_NAME)) {
            this.groupIds = Collections.singletonList(struct.getString(GROUP_ID_KEY_NAME));
        } else {
            this.groupIds = new ArrayList<>();
            Object[] groupArray = struct.getArray(GROUP_IDS_KEY_NAME);
            for (Object groupObj : groupArray)
                this.groupIds.add((String) groupObj);
        }
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        switch (versionId) {
            case 0:
                assert !groupIds.isEmpty();
                return new GroupMetadataResponse(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code(), groupIds.get(0),
                        Node.noNode());
            case 1:
                Map<String, GroupMetadataResponse.GroupMetadata> groups = new HashMap<>();
                short errorCode = Errors.forException(e).code();

                GroupMetadataResponse.GroupMetadata metadata = new GroupMetadataResponse.GroupMetadata(errorCode, Node.noNode(),
                        GroupMetadataResponse.GroupState.UNKNOWN, GroupMetadataResponse.UNKNOWN_GENERATION,
                        GroupMetadataResponse.UNKNOWN_PROTOCOL_TYPE, GroupMetadataResponse.UNKNOWN_PROTOCOL);
                for (String groupId : groupIds)
                    groups.put(groupId, metadata);
                return new GroupMetadataResponse(groups);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.GROUP_METADATA.id)));
        }
    }

    public List<String> groupIds() {
        return groupIds;
    }

    public boolean includeAllGroups() {
        return includeAllGroups;
    }

    public static GroupMetadataRequest parse(ByteBuffer buffer, int versionId) {
        return new GroupMetadataRequest(ProtoUtils.parseRequest(ApiKeys.GROUP_METADATA.id, versionId, buffer));
    }

    public static GroupMetadataRequest parse(ByteBuffer buffer) {
        return new GroupMetadataRequest((Struct) CURRENT_SCHEMA.read(buffer));
    }
}
