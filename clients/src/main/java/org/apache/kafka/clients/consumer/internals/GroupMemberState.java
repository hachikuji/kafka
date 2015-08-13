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

import org.apache.kafka.common.requests.JoinGroupRequest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is used to track the state of
 */
public class GroupMemberState {
    private final String groupType;
    private final String groupId;
    private final List<GroupProtocol> protocols;

    private enum GroupState {
        ACTIVE, INACTIVE
    }

    private String memberId;
    private int generation;
    private GroupProtocol protocol;
    private GroupState state;

    private Map<String, ByteBuffer> members;

    public GroupMemberState(String groupType,
                            String groupId,
                            List<GroupProtocol> protocols) {
        this.groupType = groupType;
        this.groupId = groupId;
        this.protocols = protocols;
    }

    public void reset() {
        if (this.state == GroupState.ACTIVE)
            leave();
        this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
    }

    public void join(String memberId,
                     int generation,
                     String protocol,
                     Map<String, ByteBuffer> members) {
        if (this.state != GroupState.INACTIVE)
            throw new IllegalStateException("Cannot join an active group");
        this.memberId = memberId;
        this.generation = generation;
        this.members = members;
    }

    public void leave() {
        if (this.state != GroupState.ACTIVE)
            throw new IllegalStateException("Cannot leave an inactive group");
        this.protocol.onGroupLeave(memberId, generation, members);
        this.state = GroupState.INACTIVE;
        this.protocol = null;
    }

    public String groupType() {
        return groupType;
    }

    public String groupId() {
        return groupId;
    }

    public List<GroupProtocol> protocols() {
        return protocols;
    }

    public int generation() {
        return generation;
    }

    public String memberId() {
        return memberId;
    }

    public Map<String, ByteBuffer> members() {
        return members;
    }

}
