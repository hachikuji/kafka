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

package kafka.server

import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.atomic.AtomicReference

import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.Node
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.metalog.MetaLogManager

import scala.jdk.CollectionConverters._

trait ControllerNodeProvider {
  def controllerNode(): Option[Node]
}

/**
 * Finds the controller node by looking in the MetadataCache.
 * This provider is used when we are in legacy mode.
 */
class MetadataCacheControllerNodeProvider(val metadataCache: kafka.server.MetadataCache,
                                          val listenerName: ListenerName) extends ControllerNodeProvider {
  override def controllerNode(): Option[Node] = {
    val image = metadataCache.currentImage()
    image.controllerId.flatMap {
      id => image.brokers.getAlive(id).flatMap {
        broker => broker.endpoints.get(listenerName.value())
      }
    }
  }
}

/**
 * Finds the controller node by checking the metadata log manager.
 * This provider is used when we are in KIP-500 mode.
 */
class RaftControllerNodeProvider(val metaLogManager: MetaLogManager,
                                 controllerQuorumVoterNodes: Seq[Node]) extends ControllerNodeProvider with Logging {
  val idToNode = controllerQuorumVoterNodes.map(node => node.id() -> node).toMap

  override def controllerNode(): Option[Node] = {
    val leader = metaLogManager.leader()
    if (leader == null) {
      None
    } else if (leader.nodeId() < 0) {
      None
    } else {
      idToNode.get(leader.nodeId())
    }
  }
}

object BrokerToControllerChannelManager {
  def apply(
    controllerNodeProvider: ControllerNodeProvider,
    time: Time,
    metrics: Metrics,
    config: KafkaConfig,
    channelName: String,
    threadNamePrefix: Option[String],
    retryTimeoutMs: Long
  ): BrokerToControllerChannelManager = {
    new BrokerToControllerChannelManagerImpl(
      controllerNodeProvider,
      time,
      metrics,
      config,
      channelName,
      threadNamePrefix,
      retryTimeoutMs
    )
  }
}

trait BrokerToControllerChannelManager {
  def start(): Unit

  def shutdown(): Unit

  def sendRequest(
    request: AbstractRequest.Builder[_ <: AbstractRequest],
    callback: ControllerRequestCompletionHandler
  ): Unit

  def controllerApiVersions(): Option[NodeApiVersions]
}

/**
 * This class manages the connection between a broker and the controller. It runs a single
 * [[BrokerToControllerRequestThread]] which uses the broker's metadata cache as its own metadata to find
 * and connect to the controller. The channel is async and runs the network connection in the background.
 * The maximum number of in-flight requests are set to one to ensure orderly response from the controller, therefore
 * care must be taken to not block on outstanding requests for too long.
 */
class BrokerToControllerChannelManagerImpl(
  controllerNodeProvider: ControllerNodeProvider,
  time: Time,
  metrics: Metrics,
  config: KafkaConfig,
  channelName: String,
  threadNamePrefix: Option[String],
  retryTimeoutMs: Long
) extends BrokerToControllerChannelManager with Logging {
  private val logContext = new LogContext(s"[broker-${config.brokerId}-to-controller] ")
  private val manualMetadataUpdater = new ManualMetadataUpdater()
  private val apiVersions = new ApiVersions()
  private val currentNodeApiVersions = NodeApiVersions.create()
  private val requestThread = newRequestThread

  def start(): Unit = {
    requestThread.start()
  }

  def shutdown(): Unit = {
    requestThread.shutdown()
    requestThread.awaitShutdown()
    info(s"Broker to controller channel manager for $channelName shutdown")
  }

  private[server] def newRequestThread = {
    val brokerToControllerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
    val brokerToControllerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)

    val networkClient = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        brokerToControllerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        brokerToControllerListenerName,
        config.saslMechanismInterBrokerProtocol,
        time,
        config.saslInterBrokerHandshakeRequestEnable,
        logContext
      )
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        channelName,
        Map("BrokerId" -> config.brokerId.toString).asJava,
        false,
        channelBuilder,
        logContext
      )
      new NetworkClient(
        selector,
        manualMetadataUpdater,
        config.brokerId.toString,
        1,
        50,
        50,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        config.connectionSetupTimeoutMs,
        config.connectionSetupTimeoutMaxMs,
        ClientDnsLookup.USE_ALL_DNS_IPS,
        time,
        true,
        apiVersions,
        logContext
      )
    }
    val threadName = threadNamePrefix match {
      case None => s"broker-${config.brokerId}-to-controller-send-thread"
      case Some(name) => s"$name:broker-${config.brokerId}-to-controller-send-thread"
    }

    new BrokerToControllerRequestThread(
      networkClient,
      manualMetadataUpdater,
      controllerNodeProvider,
      config,
      brokerToControllerListenerName,
      time,
      threadName,
      retryTimeoutMs
    )
  }

  /**
   * Send request to the controller.
   *
   * @param request         The request to be sent.
   * @param callback        Request completion callback.
   */
  def sendRequest(
    request: AbstractRequest.Builder[_ <: AbstractRequest],
    callback: ControllerRequestCompletionHandler
  ): Unit = {
    requestThread.enqueue(BrokerToControllerQueueItem(
      time.milliseconds(),
      request,
      callback
    ))
  }

  def controllerApiVersions(): Option[NodeApiVersions] =
    requestThread.activeControllerAddress().flatMap(
      activeController => if (activeController.id() == config.brokerId)
        Some(currentNodeApiVersions)
      else
        Option(apiVersions.get(activeController.idString()))
  )
}

abstract class ControllerRequestCompletionHandler extends RequestCompletionHandler {

  /**
   * Fire when the request transmission time passes the caller defined deadline on the channel queue.
   * It covers the total waiting time including retries which might be the result of individual request timeout.
   */
  def onTimeout(): Unit
}

case class BrokerToControllerQueueItem(
  createdTimeMs: Long,
  request: AbstractRequest.Builder[_ <: AbstractRequest],
  callback: ControllerRequestCompletionHandler
)

class BrokerToControllerRequestThread(
  networkClient: KafkaClient,
  metadataUpdater: ManualMetadataUpdater,
  controllerNodeProvider: ControllerNodeProvider,
  config: KafkaConfig,
  listenerName: ListenerName,
  time: Time,
  threadName: String,
  retryTimeoutMs: Long
) extends InterBrokerSendThread(threadName, networkClient, config.controllerSocketTimeoutMs, time, isInterruptible = false) {

  private val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]()
  private val activeController = new AtomicReference[Node](null)

  def activeControllerAddress(): Option[Node] = {
    Option(activeController.get())
  }

  private def updateControllerAddress(newActiveController: Node): Unit = {
    activeController.set(newActiveController)
  }

  def enqueue(request: BrokerToControllerQueueItem): Unit = {
    requestQueue.add(request)
    if (activeControllerAddress().isDefined) {
      wakeup()
    }
  }

  def queueSize: Int = {
    requestQueue.size
  }

  override def generateRequests(): Iterable[RequestAndCompletionHandler] = {
    val currentTimeMs = time.milliseconds()
    val requestIter = requestQueue.iterator()
    while (requestIter.hasNext) {
      val request = requestIter.next
      if (currentTimeMs - request.createdTimeMs >= retryTimeoutMs) {
        requestIter.remove()
        request.callback.onTimeout()
      } else {
        val controllerAddress = activeControllerAddress()
        if (controllerAddress.isDefined) {
          requestIter.remove()
          return Some(RequestAndCompletionHandler(
            time.milliseconds(),
            controllerAddress.get,
            request.request,
            handleResponse(request)
          ))
        }
      }
    }
    None
  }

  private[server] def handleResponse(request: BrokerToControllerQueueItem)(response: ClientResponse): Unit = {
    if (response.wasDisconnected()) {
      updateControllerAddress(null)
      requestQueue.putFirst(request)
    } else if (response.responseBody().errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
      // just close the controller connection and wait for metadata cache update in doWork
      activeControllerAddress().foreach { controllerAddress => {
        networkClient.disconnect(controllerAddress.idString)
        updateControllerAddress(null)
      }}

      requestQueue.putFirst(request)
    } else {
      request.callback.onComplete(response)
    }
  }

  override def doWork(): Unit = {
    if (activeControllerAddress().isDefined) {
      super.pollOnce(Long.MaxValue)
    } else {
      debug("Controller isn't cached, looking for local metadata changes")
      controllerNodeProvider.controllerNode() match {
        case Some(controller) =>
          info(s"Recorded new controller, from now on will use broker $controller")
          updateControllerAddress(controller)
          metadataUpdater.setNodes(Seq(controller).asJava)
        case None =>
          // need to backoff to avoid tight loops
          debug("No controller defined in metadata cache, retrying after backoff")
          super.pollOnce(maxTimeoutMs = 100)
      }
    }
  }
}
