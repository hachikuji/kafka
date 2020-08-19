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
package org.apache.kafka.raft;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class QuorumStateTest {
    private final int localId = 0;
    private final int logEndEpoch = 0;
    private final MockQuorumStateStore store = new MockQuorumStateStore();
    private final MockTime time = new MockTime();
    private final int electionTimeoutMs = 5000;
    private final int fetchTimeoutMs = 10000;

    private QuorumState buildQuorumState(Set<Integer> voters) {
        return new QuorumState(
            localId,
            voters,
            electionTimeoutMs,
            fetchTimeoutMs,
            store,
            time,
            new LogContext(),
            new Random(1)
        );
    }

    @Test
    public void testInitializePrimordialEpoch() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        assertTrue(state.isUnattached());
        assertEquals(0, state.epoch());
        state.transitionToCandidate();
        CandidateState candidateState = state.candidateStateOrThrow();
        assertTrue(candidateState.isVoteGranted());
        assertEquals(1, candidateState.epoch());
    }

    @Test
    public void testInitializeAsFollowerWithElectedLeader() throws IOException {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withElectedLeader(epoch, node1, voters));

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isFollower());
        assertEquals(epoch, state.epoch());

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(epoch, followerState.epoch());
        assertEquals(node1, followerState.leaderId());
    }

    @Test
    public void testInitializeAsFollowerWithVotedCandidate() throws IOException {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, node1, voters));

        QuorumState state = buildQuorumState(voters);

        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isVoted());
        assertEquals(epoch, state.epoch());

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(epoch, votedState.epoch());
        assertEquals(node1, votedState.votedId());
    }

    @Test
    public void testInitializeAsFormerCandidate() throws IOException {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, localId, voters));

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isCandidate());
        assertEquals(epoch, state.epoch());

        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(epoch, candidateState.epoch());
        assertEquals(Utils.mkSet(node1, node2), candidateState.unrecordedVoters());
    }

    @Test
    public void testInitializeAsFormerLeader() throws IOException {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withElectedLeader(epoch, localId, voters));

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isLeader());
        assertEquals(epoch, state.epoch());

        LeaderState leaderState = state.leaderStateOrThrow();
        assertEquals(epoch, leaderState.epoch());
        assertEquals(Utils.mkSet(node1, node2), leaderState.nonEndorsingFollowers());
    }

    @Test
    public void testBecomeLeader() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());

        state.transitionToLeader(0L);
        LeaderState leaderState = state.leaderStateOrThrow();
        assertTrue(state.isLeader());
        assertEquals(1, leaderState.epoch());
        assertEquals(Optional.empty(), leaderState.highWatermark());
    }

    @Test
    public void testLeaderToLeader() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());

        state.transitionToLeader(0L);
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L));
    }

    @Test
    public void testCannotBecomeLeaderIfAlreadyLeader() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        store.writeElectionState(ElectionState.withUnknownLeader(1, voters));

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.transitionToLeader(0L);
        assertTrue(state.isLeader());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L));
        assertTrue(state.isLeader());
    }

    @Test
    public void testCannotBecomeFollowerOfSelf() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());
        QuorumState state = initializeEmptyState(voters);

        assertThrows(IllegalArgumentException.class, () -> state.transitionToFollower(0, localId));
        assertThrows(IllegalArgumentException.class, () -> state.transitionToVoted(0, localId));
    }

    @Test
    public void testCannotTransitionFromUnattachedToLeader() throws IOException {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, leaderId, voters));
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isUnattached());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L));
    }

    @Test
    public void testCannotBecomeCandidateIfCurrentlyLeading() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        QuorumState state = initializeEmptyState(voters);
        state.transitionToCandidate();
        state.transitionToLeader(0L);
        assertTrue(state.isLeader());
        assertThrows(IllegalStateException.class, state::transitionToCandidate);
    }

    @Test
    public void testCannotBecomeLeaderWithoutGrantedVote() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        assertFalse(state.candidateStateOrThrow().isVoteGranted());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L));
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        assertTrue(state.candidateStateOrThrow().isVoteGranted());
        state.transitionToLeader(0L);
        assertTrue(state.isLeader());
    }

    @Test
    public void testLeaderToFollowerOfElectedLeader() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters);

        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L);
        state.transitionToFollower(5, otherNodeId);

        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(ElectionState.withElectedLeader(5, otherNodeId, voters), store.readElectionState());
    }

    private QuorumState initializeEmptyState(Set<Integer> voters) throws IOException {
        QuorumState state = buildQuorumState(voters);
        store.writeElectionState(ElectionState.withUnknownLeader(0, voters));
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        return state;
    }

    @Test
    public void testLeaderToUnattached() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L);
        state.transitionToUnattached(5);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(ElectionState.withUnknownLeader(5, voters), store.readElectionState());
    }

    @Test
    public void testLeaderToVoted() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L);
        state.transitionToVoted(5, otherNodeId);

        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        VotedState votedState = state.votedStateOrThrow();
        assertEquals(otherNodeId, votedState.votedId());
        assertEquals(ElectionState.withVotedCandidate(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testCandidateToFollower() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToFollower(5, otherNodeId);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(ElectionState.withElectedLeader(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testCandidateToUnattached() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToUnattached(5);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(ElectionState.withUnknownLeader(5, voters), store.readElectionState());
    }

    @Test
    public void testCandidateToVoted() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToVoted(5, otherNodeId);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());

        VotedState followerState = state.votedStateOrThrow();
        assertEquals(otherNodeId, followerState.votedId());
        assertEquals(ElectionState.withVotedCandidate(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testUnattachedToVotedSameEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToVoted(5, otherNodeId);

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(5, votedState.epoch());
        assertEquals(otherNodeId, votedState.votedId());
        assertEquals(ElectionState.withVotedCandidate(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testUnattachedToVotedHigherEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToVoted(8, otherNodeId);

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(8, votedState.epoch());
        assertEquals(otherNodeId, votedState.votedId());
        assertEquals(ElectionState.withVotedCandidate(8, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testVotedToVotedSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToVoted(8, node1);
        assertThrows(IllegalArgumentException.class, () -> state.transitionToVoted(8, node1));
        assertThrows(IllegalArgumentException.class, () -> state.transitionToVoted(8, node2));
    }

    @Test
    public void testVotedToFollowerSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, node1);
        state.transitionToFollower(5, node2);

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(5, node2, voters), store.readElectionState());
    }

    @Test
    public void testVotedToFollowerHigherEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, node1);
        state.transitionToFollower(8, node2);

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(8, node2, voters), store.readElectionState());
    }

    @Test
    public void testFollowerCannotChangeVotesInSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, node1);
        assertThrows(IllegalArgumentException.class, () -> state.transitionToVoted(5, node2));

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(node1, votedState.votedId());
        assertEquals(ElectionState.withVotedCandidate(5, node1, voters), store.readElectionState());
    }

    @Test
    public void testFollowerCannotChangeLeadersInSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);
        assertThrows(IllegalArgumentException.class, () -> state.transitionToFollower(8, node1));

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(8, node2, voters), store.readElectionState());
    }

    @Test
    public void testFollowerToFollowerHigherEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);
        state.transitionToFollower(9, node1);

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(9, followerState.epoch());
        assertEquals(node1, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(9, node1, voters), store.readElectionState());
    }

    @Test
    public void testCannotTransitionFromFollowerToLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        assertThrows(IllegalArgumentException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalArgumentException.class, () -> state.transitionToVoted(4, otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.transitionToFollower(4, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(ElectionState.withUnknownLeader(5, voters), store.readElectionState());
    }

    @Test
    public void testCannotTransitionFromCandidateToLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToCandidate();
        assertThrows(IllegalArgumentException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalArgumentException.class, () -> state.transitionToVoted(4, otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.transitionToFollower(4, otherNodeId));
        assertEquals(6, state.epoch());
        assertEquals(ElectionState.withVotedCandidate(6, localId, voters), store.readElectionState());
    }

    @Test
    public void testCannotTransitionFromLeaderToLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L);
        assertThrows(IllegalArgumentException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalArgumentException.class, () -> state.transitionToVoted(4, otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.transitionToFollower(4, otherNodeId));
        assertEquals(6, state.epoch());
        assertEquals(ElectionState.withElectedLeader(6, localId, voters), store.readElectionState());
    }

    @Test
    public void testCannotBecomeFollowerOfNonVoter() throws IOException {
        int otherNodeId = 1;
        int nonVoterId = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertThrows(IllegalArgumentException.class, () -> state.transitionToVoted(4, nonVoterId));
        assertThrows(IllegalArgumentException.class, () -> state.transitionToFollower(4, nonVoterId));
    }

    @Test
    public void testObserverCannotBecomeCandidateOrLeaderOrVoted() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());
        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(5, otherNodeId));
    }

    @Test
    public void testInitializeWithCorruptedStore() throws IOException {
        QuorumStateStore stateStore = Mockito.mock(QuorumStateStore.class);
        Mockito.doThrow(IOException.class).when(stateStore).readElectionState();
        QuorumState state = buildQuorumState(Utils.mkSet(localId));

        int epoch = 2;
        state.initialize(new OffsetAndEpoch(0L, epoch));
        assertEquals(epoch, state.epoch());
        assertTrue(state.isUnattached());
        assertFalse(state.hasLeader());
    }

    @Test
    public void testInconsistentVotersBetweenConfigAndState() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters);

        int unknownVoterId = 2;
        Set<Integer> stateVoters = Utils.mkSet(localId, otherNodeId, unknownVoterId);

        int epoch = 5;
        store.writeElectionState(ElectionState.withElectedLeader(epoch, localId, stateVoters));
        assertThrows(IllegalStateException.class,
            () -> state.initialize(new OffsetAndEpoch(0L, logEndEpoch)));
    }

    @Test
    public void testHasRemoteLeader() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters);
        assertFalse(state.hasRemoteLeader());

        state.transitionToCandidate();
        assertFalse(state.hasRemoteLeader());

        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L);
        assertFalse(state.hasRemoteLeader());

        state.transitionToUnattached(state.epoch() + 1);
        assertFalse(state.hasRemoteLeader());

        state.transitionToVoted(state.epoch() + 1, otherNodeId);
        assertFalse(state.hasRemoteLeader());

        state.transitionToFollower(state.epoch() + 1, otherNodeId);
        assertTrue(state.hasRemoteLeader());
    }

}
