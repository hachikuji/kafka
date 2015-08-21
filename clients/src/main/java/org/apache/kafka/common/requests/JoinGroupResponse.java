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

public class JoinGroupResponse extends AbstractRequestResponse {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.JOIN_GROUP.id);
    private static final String ERROR_CODE_KEY_NAME = "error_code";

    /**
     * Possible error code:
     *
     * GROUP_COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR_FOR_GROUP (16)
     * INCONSISTENT_GROUP_PROTOCOL (23)
     * UNKNOWN_PARTITION_ASSIGNMENT_STRATEGY (24)
     * UNKNOWN_MEMBER_ID (25)
     * INVALID_SESSION_TIMEOUT (26)
     */

    private static final String GENERATION_ID_KEY_NAME = "generation_id";
    private static final String MEMBER_ID_KEY_NAME = "member_id";
    private static final String GROUP_PROTOCOL_KEY_NAME = "group_protocol";
    private static final String GROUP_PROTOCOL_VERSION_KEY_NAME = "group_protocol_version";

    private static final String GROUP_MEMBERS_KEY_NAME = "group_members";
    private static final String PROTOCOL_METADATA_KEY_NAME = "member_metadata";

    public static final int UNKNOWN_GENERATION_ID = -1;
    public static final String UNKNOWN_GROUP_PROTOCOL = "";
    public static final short UNKNOWN_GROUP_PROTOCOL_VERSION = -1;
    public static final String UNKNOWN_MEMBER_ID = "";

    private final short errorCode;
    private final int generationId;
    private final String memberId;
    private final String groupProtocol;
    private final short groupProtocolVersion;
    private final Map<String, ByteBuffer> groupMembers;

    public JoinGroupResponse(short errorCode,
                             int generationId,
                             String memberId,
                             String groupProtocol,
                             short groupProtocolVersion,
                             Map<String, ByteBuffer> groupMembers) {
        super(new Struct(CURRENT_SCHEMA));

        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        struct.set(GENERATION_ID_KEY_NAME, generationId);
        struct.set(MEMBER_ID_KEY_NAME, memberId);
        struct.set(GROUP_PROTOCOL_KEY_NAME, groupProtocol);
        struct.set(GROUP_PROTOCOL_VERSION_KEY_NAME, groupProtocolVersion);

        List<Struct> memberArray = new ArrayList<>();
        for (Map.Entry<String, ByteBuffer> entries: groupMembers.entrySet()) {
            Struct memberData = struct.instance(GROUP_MEMBERS_KEY_NAME);
            memberData.set(MEMBER_ID_KEY_NAME, entries.getKey());
            memberData.set(PROTOCOL_METADATA_KEY_NAME, entries.getValue());
            memberArray.add(memberData);
        }
        struct.set(GROUP_MEMBERS_KEY_NAME, memberArray.toArray());

        this.errorCode = errorCode;
        this.generationId = generationId;
        this.memberId = memberId;
        this.groupProtocol = groupProtocol;
        this.groupProtocolVersion = groupProtocolVersion;
        this.groupMembers = groupMembers;
    }

    public JoinGroupResponse(Struct struct) {
        super(struct);
        groupMembers = new HashMap<>();

        for (Object memberDataObj : struct.getArray(GROUP_MEMBERS_KEY_NAME)) {
            Struct memberData = (Struct) memberDataObj;
            String memberId = memberData.getString(MEMBER_ID_KEY_NAME);
            ByteBuffer memberMetadata = memberData.getBytes(PROTOCOL_METADATA_KEY_NAME);
            groupMembers.put(memberId, memberMetadata);
        }
        errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
        generationId = struct.getInt(GENERATION_ID_KEY_NAME);
        memberId = struct.getString(MEMBER_ID_KEY_NAME);
        groupProtocol = struct.getString(GROUP_PROTOCOL_KEY_NAME);
        groupProtocolVersion = struct.getShort(GROUP_PROTOCOL_VERSION_KEY_NAME);
    }

    public short errorCode() {
        return errorCode;
    }

    public int generationId() {
        return generationId;
    }

    public String memberId() {
        return memberId;
    }

    public String groupProtocol() {
        return groupProtocol;
    }

    public short groupProtocolVersion() {
        return groupProtocolVersion;
    }

    public Map<String, ByteBuffer> groupMembers() {
        return groupMembers;
    }

    public static JoinGroupResponse parse(ByteBuffer buffer) {
        return new JoinGroupResponse((Struct) CURRENT_SCHEMA.read(buffer));
    }
}