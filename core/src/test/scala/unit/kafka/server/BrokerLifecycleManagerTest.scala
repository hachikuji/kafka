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

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.{Collections, Properties}

import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.clients.{ManualMetadataUpdater, Metadata, MockClient, NodeApiVersions}
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion
import org.apache.kafka.common.message.BrokerRegistrationRequestData.{Listener, ListenerCollection}
import org.apache.kafka.common.message.{BrokerHeartbeatResponseData, BrokerRegistrationResponseData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{BrokerHeartbeatResponse, BrokerRegistrationResponse}
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.metadata.BrokerState
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.api.Assertions._

import scala.jdk.CollectionConverters._

@Timeout(120000)
class BrokerLifecycleManagerTest {

  def configProperties = {
    val properties = new Properties()
    properties.setProperty(KafkaConfig.LogDirsProp, "/tmp/foo")
    properties.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    properties.setProperty(KafkaConfig.BrokerIdProp, "1")
    properties.setProperty(KafkaConfig.InitialBrokerRegistrationTimeoutMs, "300000")
    properties
  }

  class SimpleControllerNodeProvider extends ControllerNodeProvider {
    val node = new AtomicReference[Node](null)
    def controllerNode(): Option[Node] = Option(node.get())
  }

  private def apiVersion(
    apiKey: ApiKeys
  ): ApiVersion = {
    new ApiVersion()
      .setApiKey(apiKey.id)
      .setMinVersion(apiKey.oldestVersion)
      .setMaxVersion(apiKey.latestVersion)
  }


  class BrokerLifecycleManagerTestContext(properties: Properties) {
    val config = new KafkaConfig(properties)
    val time = new MockTime(1, 1)
    val highestMetadataOffset = new AtomicLong(0)
    val metadata = new Metadata(1000, 1000, new LogContext(), new ClusterResourceListeners())
    val mockClient = new MockClient(time, metadata)
    val metadataUpdater = new ManualMetadataUpdater()
    val controllerNodeProvider = new SimpleControllerNodeProvider()
    val channelManager = new MockBrokerToControllerChannelManager(
      mockClient,
      time,
      controllerNodeProvider,
      controllerApiVersions = NodeApiVersions.create(List(
        apiVersion(ApiKeys.BROKER_REGISTRATION),
        apiVersion(ApiKeys.BROKER_HEARTBEAT)
      ).asJava)
    )
    val clusterId = Uuid.fromString("x4AJGXQSRnephtTZzujw4w")
    val advertisedListeners = new ListenerCollection()
    config.advertisedListeners.foreach { ep =>
      advertisedListeners.add(new Listener().setHost(ep.host).
        setName(ep.listenerName.value()).
        setPort(ep.port.shortValue()).
        setSecurityProtocol(ep.securityProtocol.id))
    }
  }

  @Test
  def testCreateAndClose(): Unit = {
    val context = new BrokerLifecycleManagerTestContext(configProperties)
    val manager = new BrokerLifecycleManager(context.config, context.time, None)
    manager.close()
  }

  @Test
  def testCreateStartAndClose(): Unit = {
    val context = new BrokerLifecycleManagerTestContext(configProperties)
    val manager = new BrokerLifecycleManager(context.config, context.time, None)
    assertEquals(BrokerState.NOT_RUNNING, manager.state())
    manager.start(() => context.highestMetadataOffset.get(),
      context.channelManager, context.clusterId, context.advertisedListeners,
      Collections.emptyMap())
    TestUtils.retry(60000) {
      context.channelManager.poll()
      assertEquals(BrokerState.STARTING, manager.state())
    }
    manager.close()
    assertEquals(BrokerState.SHUTTING_DOWN, manager.state())
  }

  @Test
  def testSuccessfulRegistration(): Unit = {
    val context = new BrokerLifecycleManagerTestContext(configProperties)
    val manager = new BrokerLifecycleManager(context.config, context.time, None)
    val controllerNode = new Node(3000, "localhost", 8021)
    context.controllerNodeProvider.node.set(controllerNode)
    context.mockClient.prepareResponseFrom(new BrokerRegistrationResponse(
      new BrokerRegistrationResponseData().setBrokerEpoch(1000)), controllerNode)
    manager.start(() => context.highestMetadataOffset.get(),
      context.channelManager, context.clusterId, context.advertisedListeners,
      Collections.emptyMap())
    TestUtils.retry(10000) {
      context.channelManager.poll()
      assertEquals(1000L, manager.brokerEpoch())
    }
    manager.close()
  }

  @Test
  def testRegistrationTimeout(): Unit = {
    val context = new BrokerLifecycleManagerTestContext(configProperties)
    val controllerNode = new Node(3000, "localhost", 8021)
    val manager = new BrokerLifecycleManager(context.config, context.time, None)
    context.controllerNodeProvider.node.set(controllerNode)
    def newDuplicateRegistrationResponse(): Unit = {
      context.mockClient.prepareResponseFrom(new BrokerRegistrationResponse(
        new BrokerRegistrationResponseData().
          setErrorCode(Errors.DUPLICATE_BROKER_REGISTRATION.code())), controllerNode)
    }
    newDuplicateRegistrationResponse()
    assertEquals(1, context.mockClient.futureResponses().size)
    manager.start(() => context.highestMetadataOffset.get(),
      context.channelManager, context.clusterId, context.advertisedListeners,
      Collections.emptyMap())
    // We should send the first registration request and get a failure immediately
    TestUtils.retry(60000) {
      context.channelManager.poll()
      assertEquals(0, context.mockClient.futureResponses().size)
    }
    // Verify that we resend the registration request.
    newDuplicateRegistrationResponse()
    TestUtils.retry(60000) {
      context.time.sleep(100)
      context.channelManager.poll()
      manager.eventQueue.wakeup()
      assertEquals(0, context.mockClient.futureResponses().size)
    }
    // Verify that we time out eventually.
    context.time.sleep(300000)
    TestUtils.retry(60000) {
      context.channelManager.poll()
      manager.eventQueue.wakeup()
      assertEquals(BrokerState.SHUTTING_DOWN, manager.state())
      assertTrue(manager.initialCatchUpFuture.isCompletedExceptionally())
      assertEquals(-1L, manager.brokerEpoch())
    }
    manager.close()
  }

  @Test
  def testControlledShutdown(): Unit = {
    val context = new BrokerLifecycleManagerTestContext(configProperties)
    val manager = new BrokerLifecycleManager(context.config, context.time, None)
    val controllerNode = new Node(3000, "localhost", 8021)
    context.controllerNodeProvider.node.set(controllerNode)
    context.mockClient.prepareResponseFrom(new BrokerRegistrationResponse(
      new BrokerRegistrationResponseData().setBrokerEpoch(1000)), controllerNode)
    context.mockClient.prepareResponseFrom(new BrokerHeartbeatResponse(
      new BrokerHeartbeatResponseData().setIsCaughtUp(true)), controllerNode)
    manager.start(() => context.highestMetadataOffset.get(),
      context.channelManager, context.clusterId, context.advertisedListeners,
      Collections.emptyMap())
    TestUtils.retry(10000) {
      context.channelManager.poll()
      assertEquals(BrokerState.RECOVERY, manager.state())
    }
    context.mockClient.prepareResponseFrom(new BrokerHeartbeatResponse(
      new BrokerHeartbeatResponseData().setIsFenced(false)), controllerNode)
    context.time.sleep(20)
    TestUtils.retry(10000) {
      context.channelManager.poll()
      assertEquals(BrokerState.RUNNING, manager.state())
    }
    manager.beginControlledShutdown()
    TestUtils.retry(10000) {
      context.channelManager.poll()
      assertEquals(BrokerState.PENDING_CONTROLLED_SHUTDOWN, manager.state())
    }
    context.mockClient.prepareResponseFrom(new BrokerHeartbeatResponse(
      new BrokerHeartbeatResponseData().setShouldShutDown(true)), controllerNode)
    context.time.sleep(3000)
    TestUtils.retry(10000) {
      context.channelManager.poll()
      assertEquals(BrokerState.SHUTTING_DOWN, manager.state())
    }
    manager.controlledShutdownFuture.get()
    manager.close()
  }
}