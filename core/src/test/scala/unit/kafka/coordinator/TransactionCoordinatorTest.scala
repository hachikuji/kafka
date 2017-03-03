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
package kafka.coordinator

import kafka.common.Topic
import kafka.utils.ZkUtils
import org.apache.kafka.common.protocol.Errors
import org.apache.zookeeper.data.ACL
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.{After, Before, Test}
import org.junit.Assert._

class TransactionCoordinatorTest {

  val zkUtils: ZkUtils = EasyMock.createNiceMock(classOf[ZkUtils])

  var zkVersion: Int = -1
  var data: String = null
  val capturedVersion: Capture[Int] = EasyMock.newCapture()
  val capturedData: Capture[String] = EasyMock.newCapture()
  EasyMock.expect(zkUtils.readDataAndVersionMaybeNull(EasyMock.anyString()))
    .andAnswer(new IAnswer[(Option[String], Int)] {
      override def answer(): (Option[String], Int) = {
        if (zkVersion == -1) {
          (None.asInstanceOf[Option[String]], 0)
        } else {
          (Some(data), zkVersion)
        }
      }
    })
    .anyTimes()

  EasyMock.expect(zkUtils.conditionalUpdatePersistentPath(EasyMock.anyString(),
    EasyMock.capture(capturedData),
    EasyMock.capture(capturedVersion),
    EasyMock.anyObject().asInstanceOf[Option[(ZkUtils, String, String) => (Boolean,Int)]]))
    .andAnswer(new IAnswer[(Boolean, Int)] {
      override def answer(): (Boolean, Int) = {
        zkVersion = capturedVersion.getValue + 1
        data = capturedData.getValue

        (true, zkVersion)
      }
    })
    .anyTimes()

  EasyMock.expect(zkUtils.getTopicPartitionCount(Topic.TransactionStateTopicName))
    .andReturn(Some(2))
    .once()
  
  EasyMock.replay(zkUtils)

  val pIDManager: PidManager = new PidManager(0, zkUtils)
  val logManager: TransactionLogManager = new TransactionLogManager(0, zkUtils)
  val coordinator: TransactionCoordinator = new TransactionCoordinator(0, pIDManager, logManager)

  var result: InitPidResult = null

  @Before
  def setUp(): Unit = {
    coordinator.startup()
    // only give one of the two partitions of the transaction topic
    coordinator.handleTxnImmigration(1)
  }

  @After
  def tearDown(): Unit = {
    EasyMock.reset(zkUtils)
    coordinator.shutdown()
  }

  @Test
  def testHandleInitPID() = {
    val transactionTimeoutMs = 1000

    coordinator.handleInitPid("", transactionTimeoutMs, initPIDMockCallback)
    assertEquals(InitPidResult(0L, 0, Errors.NONE), result)

    coordinator.handleInitPid("", transactionTimeoutMs, initPIDMockCallback)
    assertEquals(InitPidResult(1L, 0, Errors.NONE), result)

    coordinator.handleInitPid("a", transactionTimeoutMs, initPIDMockCallback)
    assertEquals(InitPidResult(2L, 0, Errors.NONE), result)

    coordinator.handleInitPid("a", transactionTimeoutMs, initPIDMockCallback)
    assertEquals(InitPidResult(2L, 1, Errors.NONE), result)

    coordinator.handleInitPid("c", transactionTimeoutMs, initPIDMockCallback)
    assertEquals(InitPidResult(3L, 0, Errors.NONE), result)

    coordinator.handleInitPid("b", transactionTimeoutMs, initPIDMockCallback)
    assertEquals(InitPidResult(-1L, -1, Errors.NOT_COORDINATOR), result)
  }

  def initPIDMockCallback(ret: InitPidResult): Unit = {
    result = ret
  }
}
