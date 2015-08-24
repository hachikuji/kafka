/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.{OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.log.LogConfig
import kafka.message.UncompressedCodec
import kafka.server._
import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.JoinGroupRequest

import scala.collection.{Map, Seq, immutable}

case class GroupManagerConfig(groupMinSessionTimeoutMs: Int,
                              groupMaxSessionTimeoutMs: Int)

case class GroupProtocol(name: String, version: Short)

/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
 */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupManagerConfig,
                       val offsetConfig: OffsetManagerConfig,
                       private val offsetManager: OffsetManager,
                       zkClient: ZkClient) extends Logging {
  type JoinCallback = (Map[String, Array[Byte]], String, Int, GroupProtocol, Short) => Unit

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  private var heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat] = null
  private var rebalancePurgatory: DelayedOperationPurgatory[DelayedRebalance] = null
  private var coordinatorMetadata: CoordinatorMetadata = null

  def this(brokerId: Int,
           groupConfig: GroupManagerConfig,
           offsetConfig: OffsetManagerConfig,
           replicaManager: ReplicaManager,
           zkClient: ZkClient,
           scheduler: KafkaScheduler) = this(brokerId, groupConfig, offsetConfig,
    new OffsetManager(offsetConfig, replicaManager, zkClient, scheduler), zkClient)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, UncompressedCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup() {
    info("Starting up.")
    heartbeatPurgatory = new DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", brokerId)
    rebalancePurgatory = new DelayedOperationPurgatory[DelayedRebalance]("Rebalance", brokerId)
    coordinatorMetadata = new CoordinatorMetadata(brokerId)
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    offsetManager.shutdown()
    coordinatorMetadata.shutdown()
    heartbeatPurgatory.shutdown()
    rebalancePurgatory.shutdown()
    info("Shutdown complete.")
  }

  def handleJoinGroup(groupId: String,
                      memberId: String,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(GroupProtocol, Array[Byte])],
                      responseCallback: JoinCallback) {
    if (!isActive.get) {
      responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
               sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.INVALID_SESSION_TIMEOUT.code)
    } else {
      // only try to create the group if the group is not unknown AND
      // the member id is UNKNOWN, if member is specified but group does not
      // exist we should reject the request
      var group = coordinatorMetadata.getGroup(groupId)
      if (group == null) {
        if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) {
          responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.UNKNOWN_MEMBER_ID.code)
        } else {
          group = coordinatorMetadata.addGroup(groupId, protocolType)
          doJoinGroup(group, memberId, sessionTimeoutMs, protocols, responseCallback)
        }
      } else if (group.protocolType != protocolType) {
        responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.INCONSISTENT_GROUP_PROTOCOL.code)
      } else {
        doJoinGroup(group, memberId, sessionTimeoutMs, protocols, responseCallback)
      }
    }
  }

  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          sessionTimeoutMs: Int,
                          protocols: List[(GroupProtocol, Array[Byte])],
                          responseCallback: JoinCallback) {
    group synchronized {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // joining without the specified member id,
        responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.UNKNOWN_MEMBER_ID.code)
      } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
        // if the member trying to register with a un-recognized id, send the response to let
        // it reset its member id and retry
        responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.UNKNOWN_MEMBER_ID.code)
      } else if (!group.supportsProtocols(protocols.map(_._1).toSet)) {
        // if the group cannot support any of the member's protocols, then reject the member
        responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.INCONSISTENT_GROUP_PROTOCOL.code)
      } else if (matchesCurrentMetadata(group, memberId, protocols)) {
        /*
         * if an existing member sends a JoinGroupRequest with no changes while the group is stable,
         * just treat it like a heartbeat and return the current group metadata
         */
        val member = group.get(memberId)
        completeAndScheduleNextHeartbeatExpiration(group, member)
        responseCallback(group.currentMemberMetadata, memberId, group.generationId, group.currentProtocol,
          Errors.NONE.code)
      } else {
        val member = if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
          // if the member id is unknown, register the member to the group
          val generatedMemberId = group.generateNextMemberId
          val member = addMember(generatedMemberId, sessionTimeoutMs, protocols, group)
          maybePrepareRebalance(group)
          member
        } else {
          val member = group.get(memberId)
          if (!member.matches(protocols)) {
            // existing member changed its protocol metadata
            updateMember(group, member, protocols)
            maybePrepareRebalance(group)
            member
          } else {
            // existing member rejoining a group due to restabilize
            member
          }
        }

        member.awaitingRebalanceCallback = responseCallback

        if (group.is(PreparingRebalance))
          rebalancePurgatory.checkAndComplete(ConsumerGroupKey(group.groupId))
      }
    }
  }

  def handleHeartbeat(groupId: String,
                      memberId: String,
                      generationId: Int,
                      responseCallback: Short => Unit) {
    if (!isActive.get) {
      responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else {
      val group = coordinatorMetadata.getGroup(groupId)
      if (group == null) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // joining without the specified member id,
        responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
      } else {
        group synchronized {
          if (group.is(Dead)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else if (!group.has(memberId)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else if (generationId != group.generationId || !group.is(Stable)) {
            responseCallback(Errors.ILLEGAL_GENERATION.code)
          } else {
            val member = group.get(memberId)
            completeAndScheduleNextHeartbeatExpiration(group, member)
            responseCallback(Errors.NONE.code)
          }
        }
      }
    }
  }

  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicAndPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicAndPartition, Short] => Unit) {
    if (!isActive.get) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.NOT_COORDINATOR_FOR_GROUP.code))
    } else {
      val group = coordinatorMetadata.getGroup(groupId)
      if (group == null) {
        // if the group does not exist, it means this group is not relying
        // on Kafka for partition management, and hence never send join-group
        // request to the coordinator before; in this case blindly commit the offsets
        offsetManager.storeOffsets(groupId, memberId, generationId, offsetMetadata, responseCallback)
      } else {
        group synchronized {
          if (group.is(Dead)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
          } else if (!group.has(memberId)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
          } else if (generationId != group.generationId) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
          } else {
            offsetManager.storeOffsets(groupId, memberId, generationId, offsetMetadata, responseCallback)
          }
        }
      }
    }
  }

  def handleFetchOffsets(groupId: String,
                         partitions: Seq[TopicAndPartition]): Map[TopicAndPartition, OffsetMetadataAndError] = {
    if (!isActive.get) {
      partitions.map {case topicAndPartition => (topicAndPartition, OffsetMetadataAndError.NotCoordinatorForGroup)}.toMap
    } else if (!isCoordinatorForGroup(groupId)) {
      partitions.map {case topicAndPartition => (topicAndPartition, OffsetMetadataAndError.NotCoordinatorForGroup)}.toMap
    } else {
      val group = coordinatorMetadata.getGroup(groupId)
      if (group == null) {
        // if the group does not exist, it means this group is not relying
        // on Kafka for partition management, and hence never send join-group
        // request to the coordinator before; in this case blindly fetch the offsets
        offsetManager.getOffsets(groupId, partitions)
      } else {
        group synchronized {
          if (group.is(Dead)) {
            partitions.map {case topicAndPartition => (topicAndPartition, OffsetMetadataAndError.UnknownMember)}.toMap
          } else {
            offsetManager.getOffsets(groupId, partitions)
          }
        }
      }
    }
  }

  def handleGroupImmigration(offsetTopicPartitionId: Int) = {
    // TODO we may need to add more logic in KAFKA-2017
    offsetManager.loadOffsetsFromLog(offsetTopicPartitionId)
  }

  def handleGroupEmigration(offsetTopicPartitionId: Int) = {
    // TODO we may need to add more logic in KAFKA-2017
    offsetManager.removeOffsetsFromCacheForPartition(offsetTopicPartitionId)
  }


  /**
   * Check if the member is already part of the group and has matching metadata
   */
  private def matchesCurrentMetadata(group: GroupMetadata,
                                     memberId: String,
                                     protocols: List[(GroupProtocol, Array[Byte])]): Boolean = {
    if (!group.has(memberId) || group.not(Stable))
      return false

    protocols.find(_._1 == group.currentProtocol) match {
      case Some((protocol, data)) => group.get(memberId).matches(group.currentProtocol, data)
      case None => false
    }
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   */
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata) {
    // complete current heartbeat expectation
    member.latestHeartbeat = SystemTime.milliseconds
    val memberKey = ConsumerKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)

    // reschedule the next heartbeat expiration deadline
    val newHeartbeatDeadline = member.latestHeartbeat + member.sessionTimeoutMs
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.sessionTimeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  private def addMember(memberId: String,
                          sessionTimeoutMs: Int,
                          protocols: List[(GroupProtocol, Array[Byte])],
                          group: GroupMetadata) = {
    val member = new MemberMetadata(memberId, group.groupId, sessionTimeoutMs, protocols)
    group.add(member.memberId, member)
    member
  }

  private def removeMember(group: GroupMetadata, member: MemberMetadata) {
    group.remove(member.memberId)
  }

  private def updateMember(group: GroupMetadata, member: MemberMetadata, protocols: List[(GroupProtocol, Array[Byte])]) {
    group.remove(member.memberId)
    member.supportedProtocols = protocols
    group.add(member.memberId, member)
  }

  private def maybePrepareRebalance(group: GroupMetadata) {
    group synchronized {
      if (group.canRebalance)
        prepareRebalance(group)
    }
  }

  private def prepareRebalance(group: GroupMetadata) {
    group.transitionTo(PreparingRebalance)
    info("Preparing to restabilize group %s with old generation %s".format(group.groupId, group.generationId))

    val rebalanceTimeout = group.rebalanceTimeout
    val delayedRebalance = new DelayedRebalance(this, group, rebalanceTimeout)
    val consumerGroupKey = ConsumerGroupKey(group.groupId)
    rebalancePurgatory.tryCompleteElseWatch(delayedRebalance, Seq(consumerGroupKey))
  }

  private def restabilize(group: GroupMetadata) {
    assert(group.notYetRejoinedMembers == List.empty[MemberMetadata])

    group.transitionTo(Rebalancing)
    group.generationId += 1

    info("Restabilizing group %s with new generation %s".format(group.groupId, group.generationId))

    val groupProtocol = selectProtocol(group)
    info("Restabilize group %s generation %s has selected protocol: %s"
          .format(group.groupId, group.generationId, groupProtocol))

    group.stabilize(groupProtocol)
    info("Stabilized group %s generation %s".format(group.groupId, group.generationId))
  }

  private def onHeartbeatExpired(group: GroupMetadata, member: MemberMetadata) {
    trace("Member %s in group %s has failed".format(member.memberId, group.groupId))
    removeMember(group, member)
    maybePrepareRebalance(group)
  }

  private def selectProtocol(group: GroupMetadata): GroupProtocol = {
    // select the protocol for this group which is supported by all members
    val candidates = group.candidateProtocols

    if (candidates.isEmpty)
      throw new IllegalStateException("Attempt to create group with inconsistent protocols")

    // let each member vote for one of the protocols and choose the one with the most votes
    val votes: List[(GroupProtocol, Int)] = group.allMembers
      .map(_.vote(candidates))
      .groupBy(identity)
      .mapValues(_.size)
      .toList

    votes.sortBy(-_._2).head._1
  }

  def tryCompleteRebalance(group: GroupMetadata, forceComplete: () => Boolean) = {
    group synchronized {
      if (group.notYetRejoinedMembers == List.empty[MemberMetadata])
        forceComplete()
      else false
    }
  }

  def onExpirationRestabilize() {
    // TODO: add metrics for restabilize timeouts
  }

  def onCompleteRestabilize(group: GroupMetadata) {
    group synchronized {
      val failedMembers = group.notYetRejoinedMembers
      if (group.isEmpty || !failedMembers.isEmpty) {
        failedMembers.foreach { failedMember =>
          removeMember(group, failedMember)
          // TODO: cut the socket connection to the client
        }

        if (group.isEmpty) {
          group.transitionTo(Dead)
          info("Group %s generation %s is dead and removed".format(group.groupId, group.generationId))
          coordinatorMetadata.removeGroup(group.groupId)
        }
      }
      if (!group.is(Dead)) {
        // assign partitions to existing members of the group according to the partitioning strategy
        restabilize(group)

        // trigger the awaiting join group response callback for all the members after rebalancing
        for (member <- group.allMembers) {
          assert(member.awaitingRebalanceCallback != null)
          member.awaitingRebalanceCallback(group.currentMemberMetadata, member.memberId, group.generationId,
            group.currentProtocol, Errors.NONE.code)
          member.awaitingRebalanceCallback = null
          completeAndScheduleNextHeartbeatExpiration(group, member)
        }
      }
    }
  }

  def tryCompleteHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long, forceComplete: () => Boolean) = {
    group synchronized {
      if (shouldKeepMemberAlive(member, heartbeatDeadline))
        forceComplete()
      else false
    }
  }

  def onExpirationHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long) {
    group synchronized {
      if (!shouldKeepMemberAlive(member, heartbeatDeadline))
        onHeartbeatExpired(group, member)
    }
  }

  def onCompleteHeartbeat() {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = offsetManager.partitionFor(group)

  private def shouldKeepMemberAlive(member: MemberMetadata, heartbeatDeadline: Long) =
    member.awaitingRebalanceCallback != null || member.latestHeartbeat + member.sessionTimeoutMs > heartbeatDeadline

  private def isCoordinatorForGroup(groupId: String) = offsetManager.leaderIsLocal(offsetManager.partitionFor(groupId))
}

object GroupCoordinator {

  val NoProtocol = GroupProtocol("", -1)
  val OffsetsTopicName = "__consumer_offsets"

  def create(config: KafkaConfig,
             zkClient: ZkClient,
             replicaManager: ReplicaManager,
             kafkaScheduler: KafkaScheduler): GroupCoordinator = {
    val offsetConfig = OffsetManagerConfig(maxMetadataSize = config.offsetMetadataMaxSize,
      loadBufferSize = config.offsetsLoadBufferSize,
      offsetsRetentionMs = config.offsetsRetentionMinutes * 60 * 1000L,
      offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
      offsetsTopicNumPartitions = config.offsetsTopicPartitions,
      offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
      offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
      offsetCommitRequiredAcks = config.offsetCommitRequiredAcks)
    val groupConfig = GroupManagerConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs)

    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, replicaManager, zkClient, kafkaScheduler)
  }

  def create(config: KafkaConfig,
             zkClient: ZkClient,
             offsetManager: OffsetManager): GroupCoordinator = {
    val offsetConfig = OffsetManagerConfig(maxMetadataSize = config.offsetMetadataMaxSize,
      loadBufferSize = config.offsetsLoadBufferSize,
      offsetsRetentionMs = config.offsetsRetentionMinutes * 60 * 1000L,
      offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
      offsetsTopicNumPartitions = config.offsetsTopicPartitions,
      offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
      offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
      offsetCommitRequiredAcks = config.offsetCommitRequiredAcks)
    val groupConfig = GroupManagerConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs)

    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, offsetManager, zkClient)
  }
}
