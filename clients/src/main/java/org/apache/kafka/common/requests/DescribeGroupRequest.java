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
import java.util.List;

public class DescribeGroupRequest extends AbstractRequest {
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.DESCRIBE_GROUP.id);
    private static final String GROUP_IDS_KEY_NAME = "group_ids";
    private static final String VERBOSE_KEY_NAME = "verbose";

    private final boolean verbose;
    private final List<String> groupIds;

    public DescribeGroupRequest(
            boolean verbose,
            List<String> groupIds) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(VERBOSE_KEY_NAME, (byte) (verbose? 1 : 0));
        struct.set(GROUP_IDS_KEY_NAME, groupIds.toArray());
        this.verbose = verbose;
        this.groupIds = groupIds;
    }

    public DescribeGroupRequest(Struct struct) {
        super(struct);
        this.verbose = struct.getByte(VERBOSE_KEY_NAME) == 1;
        this.groupIds = new ArrayList<>();
        Object[] groupArray = struct.getArray(GROUP_IDS_KEY_NAME);
        for (Object groupObj : groupArray)
            this.groupIds.add((String) groupObj);
    }

    public List<String> groupIds() {
        return groupIds;
    }

    public boolean verbose() {
        return verbose;
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        switch (versionId) {
            case 0:
                // FIXME
                return null;
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.DESCRIBE_GROUP.id)));
        }
    }

    public static DescribeGroupRequest parse(ByteBuffer buffer, int versionId) {
        return new DescribeGroupRequest(ProtoUtils.parseRequest(ApiKeys.GROUP_METADATA.id, versionId, buffer));
    }

    public static DescribeGroupRequest parse(ByteBuffer buffer) {
        return new DescribeGroupRequest((Struct) CURRENT_SCHEMA.read(buffer));
    }

}
