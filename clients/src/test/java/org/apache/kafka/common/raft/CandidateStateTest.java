/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.raft;

import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CandidateStateTest {
    private final int localId = 0;
    private final int epoch = 5;

    @Test
    public void testSingleNodeQuorum() {
        CandidateState state = new CandidateState(localId, epoch, Collections.singleton(localId));
        assertTrue(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Collections.emptySet(), state.remainingVoters());
    }

    @Test
    public void testTwoNodeQuorumVoteRejected() {
        int otherNodeId = 1;
        CandidateState state = new CandidateState(localId, epoch, Utils.mkSet(localId, otherNodeId));
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Collections.singleton(otherNodeId), state.remainingVoters());
        assertTrue(state.voteRejectedBy(otherNodeId));
        assertFalse(state.isVoteGranted());
        assertTrue(state.isVoteRejected());
    }

    @Test
    public void testTwoNodeQuorumVoteGranted() {
        int otherNodeId = 1;
        CandidateState state = new CandidateState(localId, epoch, Utils.mkSet(localId, otherNodeId));
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Collections.singleton(otherNodeId), state.remainingVoters());
        assertTrue(state.voteGrantedBy(otherNodeId));
        assertEquals(Collections.emptySet(), state.remainingVoters());
        assertFalse(state.isVoteRejected());
        assertTrue(state.isVoteGranted());
    }

    @Test
    public void testThreeNodeQuorumVoteGranted() {
        int node1 = 1;
        int node2 = 2;
        CandidateState state = new CandidateState(localId, epoch, Utils.mkSet(localId, node1, node2));
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Utils.mkSet(node1, node2), state.remainingVoters());
        assertTrue(state.voteGrantedBy(node1));
        assertEquals(Collections.singleton(node2), state.remainingVoters());
        assertTrue(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertTrue(state.voteRejectedBy(node2));
        assertEquals(Collections.emptySet(), state.remainingVoters());
        assertTrue(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
    }

    @Test
    public void testThreeNodeQuorumVoteRejected() {
        int node1 = 1;
        int node2 = 2;
        CandidateState state = new CandidateState(localId, epoch, Utils.mkSet(localId, node1, node2));
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Utils.mkSet(node1, node2), state.remainingVoters());
        assertTrue(state.voteRejectedBy(node1));
        assertEquals(Collections.singleton(node2), state.remainingVoters());
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertTrue(state.voteRejectedBy(node2));
        assertEquals(Collections.emptySet(), state.remainingVoters());
        assertFalse(state.isVoteGranted());
        assertTrue(state.isVoteRejected());
    }

    @Test
    public void testCannotChangeVoteGrantedToRejected() {
        int otherNodeId = 1;
        CandidateState state = new CandidateState(localId, epoch, Utils.mkSet(localId, otherNodeId));
        assertTrue(state.voteGrantedBy(otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.voteRejectedBy(otherNodeId));
        assertTrue(state.isVoteGranted());
    }

    @Test
    public void testCannotChangeVoteRejectedToGranted() {
        int otherNodeId = 1;
        CandidateState state = new CandidateState(localId, epoch, Utils.mkSet(localId, otherNodeId));
        assertTrue(state.voteRejectedBy(otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.voteGrantedBy(otherNodeId));
        assertTrue(state.isVoteRejected());
    }

    @Test
    public void testCannotGrantOrRejectNonVoters() {
        int nonVoterId = 1;
        CandidateState state = new CandidateState(localId, epoch, Collections.singleton(localId));
        assertThrows(IllegalArgumentException.class, () -> state.voteGrantedBy(nonVoterId));
        assertThrows(IllegalArgumentException.class, () -> state.voteRejectedBy(nonVoterId));
    }

    @Test
    public void testIdempotentGrant() {
        int otherNodeId = 1;
        CandidateState state = new CandidateState(localId, epoch, Utils.mkSet(localId, otherNodeId));
        assertTrue(state.voteGrantedBy(otherNodeId));
        assertFalse(state.voteGrantedBy(otherNodeId));
    }

    @Test
    public void testIdempotentReject() {
        int otherNodeId = 1;
        CandidateState state = new CandidateState(localId, epoch, Utils.mkSet(localId, otherNodeId));
        assertTrue(state.voteRejectedBy(otherNodeId));
        assertFalse(state.voteRejectedBy(otherNodeId));
    }

}