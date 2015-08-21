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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JoinGroupRequest extends AbstractRequest {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.JOIN_GROUP.id);
    private static final String GROUP_TYPE_KEY_NAME = "group_type";
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String SESSION_TIMEOUT_KEY_NAME = "session_timeout";
    private static final String MEMBER_ID_KEY_NAME = "member_id";
    private static final String GROUP_PROTOCOLS_KEY_NAME = "group_protocols";
    private static final String PROTOCOL_KEY_NAME = "protocol";
    private static final String PROTOCOL_VERSION_KEY_NAME = "protocol_version";
    private static final String PROTOCOL_METADATA_KEY_NAME = "protocol_metadata";

    public static final String UNKNOWN_MEMBER_ID = "";

    private final String groupType;
    private final String groupId;
    private final int sessionTimeout;
    private final List<ProtocolMetadata> groupProtocols;
    private final String memberId;

    public static class ProtocolMetadata {
        public final String name;
        public final short version;
        public final ByteBuffer metadata;

        public ProtocolMetadata(
                String name,
                short version,
                ByteBuffer metadata) {
            this.name = name;
            this.version = version;
            this.metadata = metadata;
        }
    }

    public JoinGroupRequest(String groupType,
                            String groupId,
                            int sessionTimeout,
                            String memberId,
                            List<ProtocolMetadata> protocols) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(GROUP_TYPE_KEY_NAME, groupType);
        struct.set(GROUP_ID_KEY_NAME, groupId);
        struct.set(SESSION_TIMEOUT_KEY_NAME, sessionTimeout);
        struct.set(MEMBER_ID_KEY_NAME, memberId);
        List<Struct> protocolsArray = new ArrayList<>();
        for (ProtocolMetadata protocol : protocols) {
            Struct protocolStruct = struct.instance(GROUP_PROTOCOLS_KEY_NAME);
            protocolStruct.set(PROTOCOL_KEY_NAME, protocol.name);
            protocolStruct.set(PROTOCOL_VERSION_KEY_NAME, protocol.version);
            protocolStruct.set(PROTOCOL_METADATA_KEY_NAME, protocol.metadata);
            protocolsArray.add(protocolStruct);
        }
        struct.set(GROUP_PROTOCOLS_KEY_NAME, protocolsArray.toArray());
        this.groupType = groupType;
        this.groupId = groupId;
        this.sessionTimeout = sessionTimeout;
        this.memberId = memberId;
        this.groupProtocols = protocols;
    }

    public JoinGroupRequest(Struct struct) {
        super(struct);
        groupType = struct.getString(GROUP_TYPE_KEY_NAME);
        groupId = struct.getString(GROUP_ID_KEY_NAME);
        sessionTimeout = struct.getInt(SESSION_TIMEOUT_KEY_NAME);
        memberId = struct.getString(MEMBER_ID_KEY_NAME);

        Object[] protocolsArray = struct.getArray(GROUP_PROTOCOLS_KEY_NAME);
        groupProtocols = new ArrayList<>();
        for (Object protocolObj : protocolsArray) {
            Struct protocol = (Struct) protocolObj;
            String protocolName = protocol.getString(PROTOCOL_KEY_NAME);
            short protocolVersion = protocol.getShort(PROTOCOL_VERSION_KEY_NAME);
            ByteBuffer protocolMetadata = protocol.getBytes(PROTOCOL_METADATA_KEY_NAME);
            groupProtocols.add(new ProtocolMetadata(protocolName, protocolVersion, protocolMetadata));
        }
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        switch (versionId) {
            case 0:
                return new JoinGroupResponse(
                        Errors.forException(e).code(),
                        JoinGroupResponse.UNKNOWN_GENERATION_ID,
                        JoinGroupResponse.UNKNOWN_MEMBER_ID,
                        JoinGroupResponse.UNKNOWN_GROUP_PROTOCOL,
                        JoinGroupResponse.UNKNOWN_GROUP_PROTOCOL_VERSION,
                        Collections.<String, ByteBuffer>emptyMap());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.JOIN_GROUP.id)));
        }
    }

    public String groupType() {
        return groupType;
    }

    public String groupId() {
        return groupId;
    }

    public int sessionTimeout() {
        return sessionTimeout;
    }

    public List<ProtocolMetadata> groupProtocols() {
        return groupProtocols;
    }

    public String memberId() {
        return memberId;
    }

    public static JoinGroupRequest parse(ByteBuffer buffer, int versionId) {
        return new JoinGroupRequest(ProtoUtils.parseRequest(ApiKeys.JOIN_GROUP.id, versionId, buffer));
    }

    public static JoinGroupRequest parse(ByteBuffer buffer) {
        return new JoinGroupRequest((Struct) CURRENT_SCHEMA.read(buffer));
    }
}
