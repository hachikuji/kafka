/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.coordinator


import java.util.concurrent.TimeUnit

import junit.framework.Assert._
import kafka.server.{OffsetManager, ReplicaManager, KafkaConfig}
import kafka.utils.{KafkaScheduler, TestUtils}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.JoinGroupRequest
import org.easymock.EasyMock
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

import scala.collection.Map
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

/**
 * Test ConsumerCoordinator responses
 */
class GroupCoordinatorResponseTest extends JUnitSuite {
  type JoinGroupCallbackParams = (Map[String, Array[Byte]], String, Int, GroupProtocol, Short)
  type JoinGroupCallback = (Map[String, Array[Byte]], String, Int, GroupProtocol, Short) => Unit
  type HeartbeatCallbackParams = Short
  type HeartbeatCallback = Short => Unit

  val ConsumerMinSessionTimeout = 10
  val ConsumerMaxSessionTimeout = 200
  val DefaultSessionTimeout = 100
  var consumerCoordinator: GroupCoordinator = null
  var offsetManager : OffsetManager = null

  @Before
  def setUp() {
    val props = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    props.setProperty(KafkaConfig.ConsumerMinSessionTimeoutMsProp, ConsumerMinSessionTimeout.toString)
    props.setProperty(KafkaConfig.ConsumerMaxSessionTimeoutMsProp, ConsumerMaxSessionTimeout.toString)
    offsetManager = EasyMock.createStrictMock(classOf[OffsetManager])
    consumerCoordinator = GroupCoordinator.create(KafkaConfig.fromProps(props), null, offsetManager)
    consumerCoordinator.startup()
  }

  @After
  def tearDown() {
    EasyMock.reset(offsetManager)
    consumerCoordinator.shutdown()
  }

  @Test
  def testJoinGroupWrongCoordinator() {
    val groupType = "consumer"
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val protocols = List((GroupProtocol("range", 0), Array[Byte]()))

    val joinGroupResult = joinGroup(groupType, groupId, memberId, DefaultSessionTimeout, protocols, isCoordinatorForGroup = false)
    val joinGroupErrorCode = joinGroupResult._5
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupSessionTimeoutTooSmall() {
    val groupType = "consumer"
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val protocols = List((GroupProtocol("range", 0), Array[Byte]()))

    val joinGroupResult = joinGroup(groupType, groupId, memberId, ConsumerMinSessionTimeout - 1, protocols, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._5
    assertEquals(Errors.INVALID_SESSION_TIMEOUT.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupSessionTimeoutTooLarge() {
    val groupType = "consumer"
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val protocols = List((GroupProtocol("range", 0), Array[Byte]()))

    val joinGroupResult = joinGroup(groupType, groupId, memberId, ConsumerMaxSessionTimeout + 1, protocols, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._5
    assertEquals(Errors.INVALID_SESSION_TIMEOUT.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupUnknownConsumerNewGroup() {
    val groupType = "consumer"
    val groupId = "groupId"
    val memberId = "memberId"
    val protocols = List((GroupProtocol("range", 0), Array[Byte]()))

    val joinGroupResult = joinGroup(groupType, groupId, memberId, DefaultSessionTimeout, protocols, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._5
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, joinGroupErrorCode)
  }

  @Test
  def testValidJoinGroup() {
    val groupType = "consumer"
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val protocols = List((GroupProtocol("range", 0), Array[Byte]()))

    val joinGroupResult = joinGroup(groupType, groupId, memberId, DefaultSessionTimeout, protocols, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._5
    assertEquals(Errors.NONE.code, joinGroupErrorCode)
  }

  @Test
  def testJoinGroupInconsistentPartitionAssignmentStrategy() {
    val groupType = "consumer"
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val protocols = List((GroupProtocol("range", 0), Array[Byte]()))
    val otherProtocols = List((GroupProtocol("round-robin", 0), Array[Byte]()))

    val joinGroupResult = joinGroup(groupType, groupId, memberId, DefaultSessionTimeout, protocols, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._5
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val otherJoinGroupResult = joinGroup(groupType, groupId, otherMemberId, DefaultSessionTimeout, otherProtocols, isCoordinatorForGroup = true)
    val otherJoinGroupErrorCode = otherJoinGroupResult._5
    assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code, otherJoinGroupErrorCode)
  }

  @Test
  def testJoinGroupUnknownConsumerExistingGroup() {
    val groupType = "consumer"
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "memberId"
    val protocols = List((GroupProtocol("range", 0), Array[Byte]()))

    val joinGroupResult = joinGroup(groupType, groupId, memberId, DefaultSessionTimeout, protocols, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._5
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val otherJoinGroupResult = joinGroup(groupType, groupId, otherMemberId, DefaultSessionTimeout, protocols, isCoordinatorForGroup = true)
    val otherJoinGroupErrorCode = otherJoinGroupResult._5
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, otherJoinGroupErrorCode)
  }

  @Test
  def testHeartbeatWrongCoordinator() {
    val groupId = "groupId"
    val consumerId = "memberId"

    val heartbeatResult = heartbeat(groupId, consumerId, -1, isCoordinatorForGroup = false)
    assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.code, heartbeatResult)
  }

  @Test
  def testHeartbeatUnknownGroup() {
    val groupId = "groupId"
    val consumerId = "memberId"

    val heartbeatResult = heartbeat(groupId, consumerId, -1, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, heartbeatResult)
  }

  @Test
  def testHeartbeatUnknownConsumerExistingGroup() {
    val groupType = "consumer"
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = "memberId"
    val protocols = List((GroupProtocol("range", 0), Array[Byte]()))

    val joinGroupResult = joinGroup(groupType, groupId, memberId, DefaultSessionTimeout, protocols, isCoordinatorForGroup = true)
    val joinGroupErrorCode = joinGroupResult._5
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val heartbeatResult = heartbeat(groupId, otherMemberId, 1, isCoordinatorForGroup = true)
    assertEquals(Errors.UNKNOWN_MEMBER_ID.code, heartbeatResult)
  }

  @Test
  def testHeartbeatIllegalGeneration() {
    val groupType = "consumer"
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val protocols = List((GroupProtocol("range", 0), Array[Byte]()))

    val joinGroupResult = joinGroup(groupType, groupId, memberId, DefaultSessionTimeout, protocols, isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult._2
    val joinGroupErrorCode = joinGroupResult._5
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, 2, isCoordinatorForGroup = true)
    assertEquals(Errors.ILLEGAL_GENERATION.code, heartbeatResult)
  }

  @Test
  def testValidHeartbeat() {
    val groupType = "consumer"
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val protocols = List((GroupProtocol("range", 0), Array[Byte]()))

    val joinGroupResult = joinGroup(groupType, groupId, memberId, DefaultSessionTimeout, protocols, isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult._2
    val joinGroupErrorCode = joinGroupResult._5
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, 1, isCoordinatorForGroup = true)
    assertEquals(Errors.NONE.code, heartbeatResult)
  }

  @Test
  def testHeartbeatDuringRebalanceCausesIllegalGeneration() {
    val groupType = "consumer"
    val groupId = "groupId"
    val protocols = List((GroupProtocol("range", 0), Array[Byte]()))

    // First start up a group (with a slightly larger timeout to give us time to heartbeat when the rebalance starts)
    val joinGroupResult = joinGroup(groupType, groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
      DefaultSessionTimeout, protocols, isCoordinatorForGroup = true)
    val assignedConsumerId = joinGroupResult._2
    val initialGenerationId = joinGroupResult._3
    val joinGroupErrorCode = joinGroupResult._5
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    // Then join with a new consumer to trigger a rebalance
    EasyMock.reset(offsetManager)
    sendJoinGroup(groupType, groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
      DefaultSessionTimeout, protocols, isCoordinatorForGroup = true)

    // We should be in the middle of a rebalance, so the heartbeat should return illegal generation
    EasyMock.reset(offsetManager)
    val heartbeatResult = heartbeat(groupId, assignedConsumerId, initialGenerationId, isCoordinatorForGroup = true)
    assertEquals(Errors.ILLEGAL_GENERATION.code, heartbeatResult)
  }

  @Test
  def testGenerationIdIncrementsOnRebalance() {
    val groupType = "consumer"
    val groupId = "groupId"
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    val protocols = List((GroupProtocol("range", 0), Array[Byte]()))

    val joinGroupResult = joinGroup(groupType, groupId, memberId, DefaultSessionTimeout, protocols, isCoordinatorForGroup = true)
    val initialGenerationId = joinGroupResult._3
    val joinGroupErrorCode = joinGroupResult._5
    assertEquals(1, initialGenerationId)
    assertEquals(Errors.NONE.code, joinGroupErrorCode)

    EasyMock.reset(offsetManager)
    val otherJoinGroupResult = joinGroup(groupType, groupId, otherMemberId, DefaultSessionTimeout, protocols, isCoordinatorForGroup = true)
    val nextGenerationId = otherJoinGroupResult._3
    val otherJoinGroupErrorCode = otherJoinGroupResult._5
    assertEquals(2, nextGenerationId)
    assertEquals(Errors.NONE.code, otherJoinGroupErrorCode)
  }

  private def setupJoinGroupCallback: (Future[JoinGroupCallbackParams], JoinGroupCallback) = {
    val responsePromise = Promise[JoinGroupCallbackParams]
    val responseFuture = responsePromise.future
    val responseCallback: JoinGroupCallback = (members, consumerId, generationId, protocol, errorCode) =>
      responsePromise.success((members, consumerId, generationId, protocol, errorCode))
    (responseFuture, responseCallback)
  }

  private def setupHeartbeatCallback: (Future[HeartbeatCallbackParams], HeartbeatCallback) = {
    val responsePromise = Promise[HeartbeatCallbackParams]
    val responseFuture = responsePromise.future
    val responseCallback: HeartbeatCallback = errorCode => responsePromise.success(errorCode)
    (responseFuture, responseCallback)
  }

  private def sendJoinGroup(groupType: String,
                            groupId: String,
                            memberId: String,
                            sessionTimeout: Int,
                            protocols: List[(GroupProtocol, Array[Byte])],
                            isCoordinatorForGroup: Boolean): Future[JoinGroupCallbackParams] = {
    val (responseFuture, responseCallback) = setupJoinGroupCallback
    EasyMock.expect(offsetManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(offsetManager.leaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.replay(offsetManager)
    consumerCoordinator.handleJoinGroup(groupType, groupId, memberId, sessionTimeout, protocols, responseCallback)
    responseFuture
  }

  private def joinGroup(groupType: String,
                        groupId: String,
                        memberId: String,
                        sessionTimeout: Int,
                        protocols: List[(GroupProtocol, Array[Byte])],
                        isCoordinatorForGroup: Boolean): JoinGroupCallbackParams = {
    val responseFuture = sendJoinGroup(groupType, groupId, memberId, sessionTimeout, protocols, isCoordinatorForGroup)
    // should only have to wait as long as session timeout, but allow some extra time in case of an unexpected delay
    Await.result(responseFuture, Duration(sessionTimeout+100, TimeUnit.MILLISECONDS))
  }

  private def heartbeat(groupId: String,
                        consumerId: String,
                        generationId: Int,
                        isCoordinatorForGroup: Boolean): HeartbeatCallbackParams = {
    val (responseFuture, responseCallback) = setupHeartbeatCallback
    EasyMock.expect(offsetManager.partitionFor(groupId)).andReturn(1)
    EasyMock.expect(offsetManager.leaderIsLocal(1)).andReturn(isCoordinatorForGroup)
    EasyMock.replay(offsetManager)
    consumerCoordinator.handleHeartbeat(groupId, consumerId, generationId, responseCallback)
    Await.result(responseFuture, Duration(40, TimeUnit.MILLISECONDS))
  }
}
