/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.Node;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * This interface provides a way to leverage Kafka's group management protocol to implement group-specific
 * behavior. It is intended to be used in conjunction with {@link GroupCoordinator}.
 * @see ConsumerGroupController
 * @param <T> Protocol type implemented by the group
 */
public interface GroupController<T extends GroupProtocol> {

    /**
     * Unique, descriptive name for the type of group implemented by the controller (e.g. "consumer" or "copycat")
     * @return The group type
     */
    String groupType();

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes)
     * @return true if it should, false otherwise
     */
    boolean needRejoin();

    /**
     * Get the current list of protocols (and their associated metadata) supported
     * by the local member.
     * @return Non-empty list of supported protocols
     */
    List<T> protocols();

    /**
     * Invoked when a new coordinator is found.
     * @param coordinator The coordinator
     */
    void onCoordinatorFound(Node coordinator);

    /**
     * Invoked when the present coordinator has failed (e.g. due to a request timeout
     * or a disconnect).
     * @param coordinator The coordinator
     */
    void onCoordinatorDead(Node coordinator);

    /**
     * Invoked when a new group is joined.
     * @param protocol The protocol selected by the coordinator for this generation (guaranteed to
     *                 be one of the protocols returned by {@link #protocols()}.
     * @param memberId The identifier for the local member in the group.
     * @param members Map of all members (including the local one) and their respective metadata
     */
    void onJoin(T protocol,
                String memberId,
                Map<String, ByteBuffer> members);

    /**
     * Invoked when the group is left (whether because of shutdown, metadata change, stale generation, etc.)
     * @param protocol The protocol from the current generation
     * @param memberId The identifier of the local member in the group
     * @param members Map of all current members and their respective metadata
     */
    void onLeave(T protocol,
                 String memberId,
                 Map<String, ByteBuffer> members);

}
