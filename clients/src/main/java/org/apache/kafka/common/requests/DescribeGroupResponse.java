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

public class DescribeGroupResponse extends AbstractRequestResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.DESCRIBE_GROUP.id);

    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String GROUP_STATE_KEY_NAME = "state";
    private static final String PROTOCOL_TYPE_KEY_NAME = "protocol_type";
    private static final String PROTOCOL_KEY_NAME = "protocol";

    public static final String UNKNOWN_STATE = "";
    public static final String UNKNOWN_PROTOCOL_TYPE = "";
    public static final String UNKNOWN_PROTOCOL = "";

    /**
     * Possible error codes:
     *
     * GROUP_COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR_FOR_GROUP (16)
     * AUTHORIZATION_FAILED (29)
     */

    private final short errorCode;
    private final String state;
    private final String protocolType;
    private final String protocol;

    public DescribeGroupResponse(short errorCode,
                                 String state,
                                 String protocolType,
                                 String protocol) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        struct.set(GROUP_STATE_KEY_NAME, state);
        struct.set(PROTOCOL_TYPE_KEY_NAME, protocolType);
        struct.set(PROTOCOL_KEY_NAME, protocol);

        this.errorCode = errorCode;
        this.state = state;
        this.protocolType = protocolType;
        this.protocol = protocol;
    }

    public DescribeGroupResponse(Struct struct) {
        super(struct);
        this.errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
        this.state = struct.getString(GROUP_STATE_KEY_NAME);
        this.protocolType = struct.getString(PROTOCOL_TYPE_KEY_NAME);
        this.protocol = struct.getString(PROTOCOL_KEY_NAME);
    }

    public short errorCode() {
        return errorCode;
    }

    public String state() {
        return state;
    }

    public String protocolType() {
        return protocolType;
    }

    public String protocol() {
        return protocol;
    }

    public static DescribeGroupResponse parse(ByteBuffer buffer) {
        return new DescribeGroupResponse((Struct) CURRENT_SCHEMA.read(buffer));
    }

}
