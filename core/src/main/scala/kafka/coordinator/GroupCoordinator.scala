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

case class GroupManagerConfig(consumerMinSessionTimeoutMs: Int,
                              consumerMaxSessionTimeoutMs: Int)

case class GroupProtocol(name: String, version: Short)

/**
 * ConsumerCoordinator handles consumer group and consumer offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * consumer groups. Consumer groups are assigned to coordinators based on their
 * group names.
 */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupManagerConfig,
                       val offsetConfig: OffsetManagerConfig,
                       private val offsetManager: OffsetManager,
                       zkClient: ZkClient) extends Logging {
  type JoinCallback = (Map[String, Array[Byte]], String, Int, GroupProtocol, Short) => Unit

  this.logIdent = "[ConsumerCoordinator " + brokerId + "]: "

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

  def handleJoinGroup(groupType: String,
                      groupId: String,
                      memberId: String,
                      sessionTimeoutMs: Int,
                      protocols: List[(GroupProtocol, Array[Byte])],
                      responseCallback: JoinCallback) {
    if (!isActive.get) {
      responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else if (sessionTimeoutMs < groupConfig.consumerMinSessionTimeoutMs ||
               sessionTimeoutMs > groupConfig.consumerMaxSessionTimeoutMs) {
      responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.INVALID_SESSION_TIMEOUT.code)
    } else {
      // only try to create the group if the group is not unknown AND
      // the consumer id is UNKNOWN, if consumer is specified but group does not
      // exist we should reject the request
      var group = coordinatorMetadata.getGroup(groupId)
      if (group == null) {
        if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) {
          responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.UNKNOWN_MEMBER_ID.code)
        } else {
          group = coordinatorMetadata.addGroup(groupType, groupId)
          doJoinGroup(group, memberId, sessionTimeoutMs, protocols, responseCallback)
        }
      } else {
        doJoinGroup(group, memberId, sessionTimeoutMs, protocols, responseCallback)
      }
    }
  }

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

  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          sessionTimeoutMs: Int,
                          protocols: List[(GroupProtocol, Array[Byte])],
                          responseCallback: JoinCallback) {
    group synchronized {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
        // joining without specified consumer id,
        responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.UNKNOWN_MEMBER_ID.code)
      } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
        // if the consumer trying to register with a un-recognized id, send the response to let
        // it reset its consumer id and retry
        responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.UNKNOWN_MEMBER_ID.code)
      } else if (!group.supports(protocols.map(_._1).toSet)) {
        // if the group cannot support any of the member's protocols, then reject the member
        responseCallback(Map.empty, memberId, 0, GroupCoordinator.NoProtocol, Errors.INCONSISTENT_GROUP_PROTOCOL.code)
      } else if (matchesCurrentMetadata(group, memberId, protocols)) {
        /*
         * if an existing consumer sends a JoinGroupRequest with no changes while the group is stable,
         * just treat it like a heartbeat and return the current group metadata
         */
        val member = group.get(memberId)
        completeAndScheduleNextHeartbeatExpiration(group, member)
        responseCallback(group.currentMemberMetadata, memberId, group.generationId, group.currentProtocol,
          Errors.NONE.code)
      } else {
        val member = if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
          // if the consumer id is unknown, register this consumer to the group
          val generatedMemberId = group.generateNextMemberId
          val member = addMember(generatedMemberId, sessionTimeoutMs, protocols, group)
          maybePrepareRebalance(group)
          member
        } else {
          val member = group.get(memberId)
          if (!member.matches(protocols)) {
            // existing consumer changed its subscribed topics
            updateMember(group, member, protocols)
            maybePrepareRebalance(group)
            member
          } else {
            // existing consumer rejoining a group due to restabilize
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
        // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
        // joining without specified consumer id,
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
            val consumer = group.get(memberId)
            completeAndScheduleNextHeartbeatExpiration(group, consumer)
            responseCallback(Errors.NONE.code)
          }
        }
      }
    }
  }

  def handleCommitOffsets(groupId: String,
                          consumerId: String,
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
        offsetManager.storeOffsets(groupId, consumerId, generationId, offsetMetadata, responseCallback)
      } else {
        group synchronized {
          if (group.is(Dead)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
          } else if (!group.has(consumerId)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
          } else if (generationId != group.generationId) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
          } else {
            offsetManager.storeOffsets(groupId, consumerId, generationId, offsetMetadata, responseCallback)
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
            partitions.map {case topicAndPartition => (topicAndPartition, OffsetMetadataAndError.UnknownConsumer)}.toMap
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
   * Complete existing DelayedHeartbeats for the given consumer and schedule the next one
   */
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, consumer: MemberMetadata) {
    // complete current heartbeat expectation
    consumer.latestHeartbeat = SystemTime.milliseconds
    val consumerKey = ConsumerKey(consumer.groupId, consumer.memberId)
    heartbeatPurgatory.checkAndComplete(consumerKey)

    // reschedule the next heartbeat expiration deadline
    val newHeartbeatDeadline = consumer.latestHeartbeat + consumer.sessionTimeoutMs
    val delayedHeartbeat = new DelayedHeartbeat(this, group, consumer, newHeartbeatDeadline, consumer.sessionTimeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(consumerKey))
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
    assert(group.notYetRejoinedConsumers == List.empty[MemberMetadata])

    group.transitionTo(Rebalancing)
    group.generationId += 1

    info("Restabilizing group %s with new generation %s".format(group.groupId, group.generationId))

    val groupProtocol = selectProtocol(group)
    info("Restabilize group %s generation %s has selected protocol: %s"
          .format(group.groupId, group.generationId, groupProtocol))

    group.stabilize(groupProtocol)
    info("Stabilized group %s generation %s".format(group.groupId, group.generationId))
  }

  private def onHeartbeatExpired(group: GroupMetadata, consumer: MemberMetadata) {
    trace("Consumer %s in group %s has failed".format(consumer.memberId, group.groupId))
    removeMember(group, consumer)
    maybePrepareRebalance(group)
  }

  private def selectProtocol(group: GroupMetadata): GroupProtocol = {
    val candidates = group.candidates

    if (candidates.isEmpty)
      throw new IllegalStateException("Attempt to create group with inconsistent protocols")

    val votes: List[(GroupProtocol, Int)] = group.allMembers
      .map(_.vote(candidates))
      .groupBy(identity)
      .mapValues(_.size)
      .toList

    votes.sortBy(-_._2).head._1
  }

  def tryCompleteRebalance(group: GroupMetadata, forceComplete: () => Boolean) = {
    group synchronized {
      if (group.notYetRejoinedConsumers == List.empty[MemberMetadata])
        forceComplete()
      else false
    }
  }

  def onExpirationRestabilize() {
    // TODO: add metrics for restabilize timeouts
  }

  def onCompleteRestabilize(group: GroupMetadata) {
    group synchronized {
      val failedConsumers = group.notYetRejoinedConsumers
      if (group.isEmpty || !failedConsumers.isEmpty) {
        failedConsumers.foreach { failedConsumer =>
          removeMember(group, failedConsumer)
          // TODO: cut the socket connection to the consumer
        }

        if (group.isEmpty) {
          group.transitionTo(Dead)
          info("Group %s generation %s is dead and removed".format(group.groupId, group.generationId))
          coordinatorMetadata.removeGroup(group.groupId)
        }
      }
      if (!group.is(Dead)) {
        // assign partitions to existing consumers of the group according to the partitioning strategy
        restabilize(group)

        // trigger the awaiting join group response callback for all the consumers after rebalancing
        for (consumer <- group.allMembers) {
          assert(consumer.awaitingRebalanceCallback != null)
          consumer.awaitingRebalanceCallback(group.currentMemberMetadata, consumer.memberId, group.generationId,
            group.currentProtocol, Errors.NONE.code)
          consumer.awaitingRebalanceCallback = null
          completeAndScheduleNextHeartbeatExpiration(group, consumer)
        }
      }
    }
  }

  def tryCompleteHeartbeat(group: GroupMetadata, consumer: MemberMetadata, heartbeatDeadline: Long, forceComplete: () => Boolean) = {
    group synchronized {
      if (shouldKeepConsumerAlive(consumer, heartbeatDeadline))
        forceComplete()
      else false
    }
  }

  def onExpirationHeartbeat(group: GroupMetadata, consumer: MemberMetadata, heartbeatDeadline: Long) {
    group synchronized {
      if (!shouldKeepConsumerAlive(consumer, heartbeatDeadline))
        onHeartbeatExpired(group, consumer)
    }
  }

  def onCompleteHeartbeat() {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = offsetManager.partitionFor(group)

  private def shouldKeepConsumerAlive(consumer: MemberMetadata, heartbeatDeadline: Long) =
    consumer.awaitingRebalanceCallback != null || consumer.latestHeartbeat + consumer.sessionTimeoutMs > heartbeatDeadline

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
    val groupConfig = GroupManagerConfig(consumerMinSessionTimeoutMs = config.consumerMinSessionTimeoutMs,
      consumerMaxSessionTimeoutMs = config.consumerMaxSessionTimeoutMs)

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
    val groupConfig = GroupManagerConfig(consumerMinSessionTimeoutMs = config.consumerMinSessionTimeoutMs,
      consumerMaxSessionTimeoutMs = config.consumerMaxSessionTimeoutMs)

    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, offsetManager, zkClient)
  }
}
