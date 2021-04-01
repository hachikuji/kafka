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

package kafka.server

import java.util.{Optional, Properties}

import integration.kafka.server.IntegrationTestUtils
import integration.kafka.server.IntegrationTestUtils.createTopic
import kafka.api.IntegrationTestHarness
import kafka.network.SocketServer
import kafka.test.{ClusterConfig, ClusterInstance}
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.utils.TestUtils
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{MetadataRequest, MetadataResponse}
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.test.TestUtils.isValidClusterId
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith

import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(
  clusterType = Type.BOTH,
  brokers = 2,
  autoCreateOffsetsTopic = false,
  serverProperties = Array(
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "default.replication.factor", value = "2"),
  )
)
class MetadataRequestTest(cluster: ClusterInstance) {

  @BeforeEach
  def setUp(config: ClusterConfig): Unit = {
    config.setServerOverrides { spec =>
      val props = new Properties()
      props.setProperty(KafkaConfig.RackProp, s"rack/${spec.nodeId}")
      props
    }
  }

  @ClusterTest
  def testClusterIdWithRequestVersion1(): Unit = {
    val v1MetadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
    val v1ClusterId = v1MetadataResponse.clusterId
    assertNull(v1ClusterId, s"v1 clusterId should be null")
  }

  @ClusterTest
  def testClusterIdIsValid(): Unit = {
    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(2.toShort))
    isValidClusterId(metadataResponse.clusterId)
  }

  @ClusterTest(clusterType = Type.ZK)
  def testControllerId(): Unit = {
    val harness = cluster.getUnderlying(classOf[IntegrationTestHarness])
    val controllerServer = harness.servers.find(_.kafkaController.isActive).get

    val controllerId = controllerServer.config.brokerId
    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))

    assertEquals(controllerId,
      metadataResponse.controller.id, "Controller id should match the active controller")

    // Fail over the controller
    controllerServer.shutdown()
    controllerServer.startup()

    val controllerServer2 = harness.servers.find(_.kafkaController.isActive).get
    val controllerId2 = controllerServer2.config.brokerId
    assertNotEquals(controllerId, controllerId2, "Controller id should switch to a new broker")

    TestUtils.waitUntilTrue(() => {
      val metadataResponse2 = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
      metadataResponse2.controller != null && controllerServer2.dataPlaneRequestProcessor.brokerId == metadataResponse2.controller.id
    }, "Controller id should match the active controller after failover", 5000)
  }

  @ClusterTest
  def testRack(): Unit = {
    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
    // Validate rack matches what's set in generateConfigs() above
    metadataResponse.brokers.forEach { broker =>
      assertEquals(s"rack/${broker.id}", broker.rack,
        s"Rack information for broker ${broker.id} should match config")
    }
  }

  @ClusterTest
  def testIsInternal(): Unit = {
    val internalTopic = Topic.GROUP_METADATA_TOPIC_NAME
    val notInternalTopic = "notInternal"
    // create the topics
    val admin = cluster.createAdminClient()
    createTopic(admin, internalTopic, numPartitions = 3, replicationFactor = 2)
    createTopic(admin, notInternalTopic, numPartitions = 3, replicationFactor = 2)

    val metadataResponse = awaitTopicMetadata(Seq(internalTopic, notInternalTopic))
    assertTrue(metadataResponse.errors.isEmpty, "Response should have no errors")

    val topicMetadata = metadataResponse.topicMetadata.asScala
    val internalTopicMetadata = topicMetadata.find(_.topic == internalTopic).get
    val notInternalTopicMetadata = topicMetadata.find(_.topic == notInternalTopic).get

    assertTrue(internalTopicMetadata.isInternal, "internalTopic should show isInternal")
    assertFalse(notInternalTopicMetadata.isInternal, "notInternalTopic topic not should show isInternal")

    assertEquals(Set(internalTopic).asJava, metadataResponse.cluster.internalTopics)
  }

  @ClusterTest
  def testNoTopicsRequest(): Unit = {
    // create some topics
    val admin = cluster.createAdminClient()
    createTopic(admin, "t1", numPartitions = 3, replicationFactor = 2)
    createTopic(admin, "t2", numPartitions = 3, replicationFactor = 2)

    // v0, Doesn't support a "no topics" request
    // v1, Empty list represents "no topics"
    val metadataResponse = sendMetadataRequest(new MetadataRequest.Builder(List[String]().asJava, true, 1.toShort).build)
    assertTrue(metadataResponse.errors.isEmpty, "Response should have no errors")
    assertTrue(metadataResponse.topicMetadata.isEmpty, "Response should have no topics")
  }

  private def awaitTopicMetadata(topics: Seq[String]): MetadataResponse = {
    TestUtils.awaitValue(() => {
      val response = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
      if (response.topicMetadata.size > topics.size) {
        val unexpectedTopics = response.topicMetadata.asScala.map(_.topic).toSet
            .diff(topics.toSet)
        fail(s"Found unexpected topics in $unexpectedTopics. Expected only $topics")
      } else if (response.topicMetadata.size == topics.size) {
        Some(response)
      } else {
        None
      }
    }, s"Failed to fetch metadata include topics $topics")
  }

  @ClusterTest
  def testAutoTopicCreation(): Unit = {
    val topic1 = "t1"
    val topic2 = "t2"
    val topic3 = "t3"
    val topic4 = "t4"
    val topic5 = "t5"
    val admin = cluster.createAdminClient()
    createTopic(admin, topic1, numPartitions = 1, replicationFactor = 1)

    sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic2).asJava, true).build())
    awaitTopicMetadata(Seq(topic1, topic2))

    // The default behavior in old versions of the metadata API is to allow topic creation, so
    // protocol downgrades should happen gracefully when auto-creation is explicitly requested.
    sendMetadataRequest(new MetadataRequest.Builder(Seq(topic3).asJava, true).build(1))
    // TODO: old code validated LEADER_NOT_AVAILABLE error. is that useful?
    awaitTopicMetadata(Seq(topic1, topic2, topic3))

    // V3 doesn't support a configurable allowAutoTopicCreation, so disabling auto-creation is not supported
    assertThrows(classOf[UnsupportedVersionException], () => sendMetadataRequest(new MetadataRequest(requestData(List(topic4), false), 3.toShort)))

    // V4 and higher support a configurable allowAutoTopicCreation
    val response3 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic4, topic5).asJava, false, 4.toShort).build)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response3.errors.get(topic4))
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response3.errors.get(topic5))
    awaitTopicMetadata(Seq(topic1, topic2, topic3))
  }

  @ClusterTest(
    clusterType = Type.ZK, // TODO: Raft cluster returns different error code because it just forwards to controller
    serverProperties = Array(new ClusterConfigProperty(key = "default.replication.factor", value = "3"))
  )
  def testAutoCreateTopicWithInvalidReplicationFactor(): Unit = {
    val topic1 = "testAutoCreateTopic"
    val response1 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1).asJava, true).build)
    assertEquals(1, response1.topicMetadata.size)
    val topicMetadata = response1.topicMetadata.asScala.head
    assertEquals(Errors.INVALID_REPLICATION_FACTOR, topicMetadata.error)
    assertEquals(topic1, topicMetadata.topic)
    assertEquals(0, topicMetadata.partitionMetadata.size)
  }

  @ClusterTest(clusterType = Type.ZK)
  def testAutoCreateOfCollidingTopics(): Unit = {
    val harness = cluster.getUnderlying(classOf[IntegrationTestHarness])
    val topic1 = "testAutoCreate.Topic"
    val topic2 = "testAutoCreate_Topic"
    val response1 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic2).asJava, true).build)
    assertEquals(2, response1.topicMetadata.size)

    val responseMap = response1.topicMetadata.asScala.map(metadata => (metadata.topic(), metadata.error)).toMap

    assertEquals(Set(topic1, topic2), responseMap.keySet)
    // The topic creation will be delayed, and the name collision error will be swallowed.
    assertEquals(Set(Errors.LEADER_NOT_AVAILABLE, Errors.INVALID_TOPIC_EXCEPTION), responseMap.values.toSet)

    val topicCreated = responseMap.head._1
    TestUtils.waitUntilLeaderIsElectedOrChanged(harness.zkClient, topicCreated, 0)
    TestUtils.waitForPartitionMetadata(harness.servers, topicCreated, 0)

    // retry the metadata for the first auto created topic
    val response2 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topicCreated).asJava, true).build)
    val topicMetadata1 = response2.topicMetadata.asScala.head
    assertEquals(Errors.NONE, topicMetadata1.error)
    assertEquals(Seq(Errors.NONE), topicMetadata1.partitionMetadata.asScala.map(_.error))
    assertEquals(1, topicMetadata1.partitionMetadata.size)
    val partitionMetadata = topicMetadata1.partitionMetadata.asScala.head
    assertEquals(0, partitionMetadata.partition)
    assertEquals(2, partitionMetadata.replicaIds.size)
    assertTrue(partitionMetadata.leaderId.isPresent)
    assertTrue(partitionMetadata.leaderId.get >= 0)
  }

  @ClusterTest
  def testAllTopicsRequest(): Unit = {
    // create some topics
    val admin = cluster.createAdminClient()
    createTopic(admin, "t1", numPartitions = 3, replicationFactor = 2)
    createTopic(admin, "t2", numPartitions = 3, replicationFactor = 2)
    awaitTopicMetadata(Seq("t1", "t2"))

    // v0, Empty list represents all topics
    val metadataResponseV0 = sendMetadataRequest(new MetadataRequest(requestData(List(), true), 0.toShort))
    assertTrue(metadataResponseV0.errors.isEmpty, "V0 Response should have no errors")
    assertEquals(2, metadataResponseV0.topicMetadata.size(), "V0 Response should have 2 (all) topics")

    // v1, Null represents all topics
    val metadataResponseV1 = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
    assertTrue(metadataResponseV1.errors.isEmpty, "V1 Response should have no errors")
    assertEquals(2, metadataResponseV1.topicMetadata.size(), "V1 Response should have 2 (all) topics")
  }

  @ClusterTest
  def testTopicIdsInResponse(): Unit = {
    val topic1 = "topic1"
    val topic2 = "topic2"
    val admin = cluster.createAdminClient()
    createTopic(admin, topic1, numPartitions = 2, replicationFactor = 2)
    createTopic(admin, topic2, numPartitions = 2, replicationFactor = 2)
    awaitTopicMetadata(Seq(topic1, topic2))

    // if version < 9, return ZERO_UUID in MetadataResponse
    val resp1 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic2).asJava, true, 0, 9).build())
    assertEquals(2, resp1.topicMetadata.size)
    resp1.topicMetadata.forEach { topicMetadata =>
      assertEquals(Errors.NONE, topicMetadata.error)
      assertEquals(Uuid.ZERO_UUID, topicMetadata.topicId())
    }

    // from version 10, UUID will be included in MetadataResponse
    val resp2 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic2).asJava, true, 10, 10).build())
    assertEquals(2, resp2.topicMetadata.size)
    resp2.topicMetadata.forEach { topicMetadata =>
      assertEquals(Errors.NONE, topicMetadata.error)
      assertNotEquals(Uuid.ZERO_UUID, topicMetadata.topicId())
      assertNotNull(topicMetadata.topicId())
    }
  }

  /**
    * Preferred replica should be the first item in the replicas list
    */
  @ClusterTest
  def testPreferredReplica(): Unit = {
    val replicaAssignment = Map(0 -> Seq(1, 0), 1 -> Seq(0, 1))
    val admin = cluster.createAdminClient()
    createTopic(admin, "t1", replicaAssignment)
    awaitTopicMetadata(Seq("t1"))

    val response = sendMetadataRequest(new MetadataRequest.Builder(Seq("t1").asJava, false).build())
    assertEquals(1, response.topicMetadata.size)

    val topicMetadata = response.topicMetadata.iterator.next()
    assertEquals(Errors.NONE, topicMetadata.error)
    assertEquals("t1", topicMetadata.topic)
    assertEquals(Set(0, 1), topicMetadata.partitionMetadata.asScala.map(_.partition).toSet)
    topicMetadata.partitionMetadata.forEach { partitionMetadata =>
      val assignment = replicaAssignment(partitionMetadata.partition)
      assertEquals(assignment, partitionMetadata.replicaIds.asScala)
      assertEquals(assignment, partitionMetadata.inSyncReplicaIds.asScala)
      assertEquals(Optional.of(assignment.head), partitionMetadata.leaderId)
    }
  }

  @ClusterTest(clusterType = Type.ZK, brokers = 3)
  def testReplicaDownResponse(): Unit = {
    val harness = cluster.getUnderlying(classOf[IntegrationTestHarness])
    val replicaDownTopic = "replicaDown"
    val replicaCount = 3.toShort

    // create a topic with 3 replicas
    val admin = cluster.createAdminClient()
    createTopic(admin, replicaDownTopic, numPartitions = 1, replicaCount)
    awaitTopicMetadata(Seq(replicaDownTopic))

    // Kill a replica node that is not the leader
    val metadataResponse = sendMetadataRequest(new MetadataRequest.Builder(List(replicaDownTopic).asJava, true).build())
    val partitionMetadata = metadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    val downNode = harness.servers.find { server =>
      val serverId = server.dataPlaneRequestProcessor.brokerId
      val leaderId = partitionMetadata.leaderId
      val replicaIds = partitionMetadata.replicaIds.asScala
      leaderId.isPresent && leaderId.get() != serverId && replicaIds.contains(serverId)
    }.get
    downNode.shutdown()

    TestUtils.waitUntilTrue(() => {
      val response = sendMetadataRequest(new MetadataRequest.Builder(List(replicaDownTopic).asJava, true).build())
      !response.brokers.asScala.exists(_.id == downNode.dataPlaneRequestProcessor.brokerId)
    }, "Replica was not found down", 5000)

    // Validate version 0 still filters unavailable replicas and contains error
    val v0MetadataResponse = sendMetadataRequest(new MetadataRequest(requestData(List(replicaDownTopic), true), 0.toShort))
    val v0BrokerIds = v0MetadataResponse.brokers().asScala.map(_.id).toSeq
    assertTrue(v0MetadataResponse.errors.isEmpty, "Response should have no errors")
    assertFalse(v0BrokerIds.contains(downNode.config.brokerId), s"The downed broker should not be in the brokers list")
    assertTrue(v0MetadataResponse.topicMetadata.size == 1, "Response should have one topic")
    val v0PartitionMetadata = v0MetadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    assertTrue(v0PartitionMetadata.error == Errors.REPLICA_NOT_AVAILABLE, "PartitionMetadata should have an error")
    assertTrue(v0PartitionMetadata.replicaIds.size == replicaCount - 1, s"Response should have ${replicaCount - 1} replicas")

    // Validate version 1 returns unavailable replicas with no error
    val v1MetadataResponse = sendMetadataRequest(new MetadataRequest.Builder(List(replicaDownTopic).asJava, true).build(1))
    val v1BrokerIds = v1MetadataResponse.brokers().asScala.map(_.id).toSeq
    assertTrue(v1MetadataResponse.errors.isEmpty, "Response should have no errors")
    assertFalse(v1BrokerIds.contains(downNode.config.brokerId), s"The downed broker should not be in the brokers list")
    assertEquals(1, v1MetadataResponse.topicMetadata.size, "Response should have one topic")
    val v1PartitionMetadata = v1MetadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    assertEquals(Errors.NONE, v1PartitionMetadata.error, "PartitionMetadata should have no errors")
    assertEquals(replicaCount, v1PartitionMetadata.replicaIds.size, s"Response should have $replicaCount replicas")
  }

  @ClusterTest(clusterType = Type.ZK, brokers = 3)
  def testIsrAfterBrokerShutDownAndJoinsBack(): Unit = {
    val harness = cluster.getUnderlying(classOf[IntegrationTestHarness])

    def checkIsr(topic: String): Unit = {
      val activeBrokers = harness.servers.filter(_.brokerState != BrokerState.NOT_RUNNING)
      val expectedIsr = activeBrokers.map(_.config.brokerId).toSet

      // Assert that topic metadata at new brokers is updated correctly
      activeBrokers.foreach { broker =>
        var actualIsr = Set.empty[Int]
        TestUtils.waitUntilTrue(() => {
          val metadataResponse = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic).asJava, false).build,
            Some(cluster.brokerSocketServer(broker.config.brokerId)))
          val firstPartitionMetadata = metadataResponse.topicMetadata.asScala.headOption.flatMap(_.partitionMetadata.asScala.headOption)
          actualIsr = firstPartitionMetadata.map { partitionMetadata =>
            partitionMetadata.inSyncReplicaIds.asScala.map(Int.unbox).toSet
          }.getOrElse(Set.empty)
          expectedIsr == actualIsr
        }, s"Topic metadata not updated correctly in broker $broker\n" +
          s"Expected ISR: $expectedIsr \n" +
          s"Actual ISR : $actualIsr")
      }
    }

    cluster.anyBrokerSocketServer()

    val topic = "isr-after-broker-shutdown"
    val replicaCount = 3.toShort
    val admin = cluster.createAdminClient()
    createTopic(admin, topic, numPartitions = 1, replicaCount)

    harness.servers.last.shutdown()
    harness.servers.last.awaitShutdown()
    harness.servers.last.startup()

    checkIsr(topic)
  }

  @ClusterTest(clusterType = Type.ZK, brokers = 3)
  def testAliveBrokersWithNoTopics(): Unit = {
    val harness = cluster.getUnderlying(classOf[IntegrationTestHarness])

    def checkMetadata(servers: Seq[KafkaServer], expectedBrokersCount: Int): Unit = {
      var controllerMetadataResponse: Option[MetadataResponse] = None
      TestUtils.waitUntilTrue(() => {
        val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build,
          Some(cluster.anyControllerSocketServer()))
        controllerMetadataResponse = Some(metadataResponse)
        metadataResponse.brokers.size == expectedBrokersCount
      }, s"Expected $expectedBrokersCount brokers, but there are ${controllerMetadataResponse.get.brokers.size} " +
        "according to the Controller")

      val brokersInController = controllerMetadataResponse.get.brokers.asScala.toSeq.sortBy(_.id)

      // Assert that metadata is propagated correctly
      servers.filter(_.brokerState != BrokerState.NOT_RUNNING).foreach { broker =>
        TestUtils.waitUntilTrue(() => {
          val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build,
            Some(cluster.brokerSocketServer(broker.config.brokerId)))
          val brokers = metadataResponse.brokers.asScala.toSeq.sortBy(_.id)
          val topicMetadata = metadataResponse.topicMetadata.asScala.toSeq.sortBy(_.topic)
          brokersInController == brokers && metadataResponse.topicMetadata.asScala.toSeq.sortBy(_.topic) == topicMetadata
        }, s"Topic metadata not updated correctly")
      }
    }

    val serverToShutdown = harness.servers.filterNot(_.kafkaController.isActive).last
    serverToShutdown.shutdown()
    serverToShutdown.awaitShutdown()
    checkMetadata(harness.servers.toSeq, harness.servers.size - 1)

    serverToShutdown.startup()
    checkMetadata(harness.servers.toSeq, harness.servers.size)
  }

  protected def requestData(topics: List[String], allowAutoTopicCreation: Boolean): MetadataRequestData = {
    val data = new MetadataRequestData
    if (topics == null)
      data.setTopics(null)
    else
      topics.foreach(topic =>
        data.topics.add(
          new MetadataRequestData.MetadataRequestTopic()
            .setName(topic)))

    data.setAllowAutoTopicCreation(allowAutoTopicCreation)
    data
  }

  protected def sendMetadataRequest(request: MetadataRequest, destination: Option[SocketServer] = None): MetadataResponse = {
    val server = destination.getOrElse(
      cluster.anyBrokerSocketServer()
    )

    val socket = IntegrationTestUtils.connect(server, cluster.clientListener)
    try {
      IntegrationTestUtils.sendAndReceive[MetadataResponse](request, socket)
    } finally socket.close()
  }

  protected def checkAutoCreatedTopic(autoCreatedTopic: String, response: MetadataResponse): Unit = {
    assertEquals(Errors.LEADER_NOT_AVAILABLE, response.errors.get(autoCreatedTopic))
    TestUtils.waitUntilTrue(() => {
      sendMetadataRequest(
        
      )
    }, "")


    // TODO: We need a good way to test topic partition counts
    //    assertEquals(Some(servers.head.config.numPartitions), zkClient.getTopicPartitionCount(autoCreatedTopic))
    //    for (i <- 0 until servers.head.config.numPartitions)
    //      TestUtils.waitForPartitionMetadata(servers, autoCreatedTopic, i)
  }

}
