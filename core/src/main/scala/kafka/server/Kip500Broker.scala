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

import java.util
import java.util.concurrent.{CompletableFuture, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.{ProducerIdGenerator, TransactionCoordinator}
import kafka.log.LogManager
import kafka.metrics.KafkaYammerMetrics
import kafka.network.SocketServer
import kafka.security.CredentialProvider
import kafka.server.KafkaBroker.metricsPrefix
import kafka.server.metadata.{BrokerMetadataListener, LocalConfigRepository}
import kafka.utils.{CoreUtils, KafkaScheduler}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.BrokerRegistrationRequestData.{Listener, ListenerCollection}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time}
import org.apache.kafka.common.{ClusterResource, Endpoint, KafkaException}
import org.apache.kafka.metadata.{BrokerState, VersionRange}
import org.apache.kafka.metalog.MetaLogManager
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.server.authorizer.Authorizer

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

/**
 * A KIP-500 Kafka broker.
 */
class Kip500Broker(
  val config: KafkaConfig,
  val metaProps: MetaProperties,
  val metaLogManager: MetaLogManager,
  val time: Time,
  val metrics: Metrics,
  val threadNamePrefix: Option[String],
  val initialOfflineDirs: Seq[String],
  val controllerQuorumVotersFuture: CompletableFuture[util.List[String]],
  val supportedFeatures: util.Map[String, VersionRange]
) extends KafkaBroker {

  import kafka.server.Server._

  private val logContext: LogContext = new LogContext(s"[Kip500Broker id=${config.brokerId}] ")

  this.logIdent = logContext.logPrefix

  val lifecycleManager: BrokerLifecycleManager =
      new BrokerLifecycleManager(config, time, threadNamePrefix)

  private val isShuttingDown = new AtomicBoolean(false)

  val lock = new ReentrantLock()
  val awaitShutdownCond = lock.newCondition()
  var status: ProcessStatus = SHUTDOWN

  var dataPlaneRequestProcessor: KafkaApis = null
  var controlPlaneRequestProcessor: KafkaApis = null

  var authorizer: Option[Authorizer] = None
  var socketServer: SocketServer = null
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = null
  var controlPlaneRequestHandlerPool: KafkaRequestHandlerPool = null

  var logDirFailureChannel: LogDirFailureChannel = null
  var logManager: LogManager = null

  var tokenManager: DelegationTokenManager = null

  var replicaManager: ReplicaManager = null

  var credentialProvider: CredentialProvider = null
  var tokenCache: DelegationTokenCache = null

  var groupCoordinator: GroupCoordinator = null

  var transactionCoordinator: TransactionCoordinator = null

  var forwardingChannelManager: BrokerToControllerChannelManager = null

  var alterIsrChannelManager: BrokerToControllerChannelManager = null

  var kafkaScheduler: KafkaScheduler = null

  var metadataCache: MetadataCache = null
  var quotaManagers: QuotaFactory.QuotaManagers = null

  private var _brokerTopicStats: BrokerTopicStats = null

  val brokerFeatures: BrokerFeatures = BrokerFeatures.createDefault()

  val featureCache: FinalizedFeatureCache = new FinalizedFeatureCache(brokerFeatures)

  val clusterId: String = metaProps.clusterId.toString

  val configRepository = new LocalConfigRepository()

  var brokerMetadataListener: BrokerMetadataListener = null

  def kafkaYammerMetrics: kafka.metrics.KafkaYammerMetrics = KafkaYammerMetrics.INSTANCE

  private[kafka] def brokerTopicStats = _brokerTopicStats

  private def maybeChangeStatus(from: ProcessStatus, to: ProcessStatus): Boolean = {
    lock.lock()
    try {
      if (status != from) return false
      status = to
      if (to == SHUTTING_DOWN) {
        isShuttingDown.set(true)
      } else if (to == SHUTDOWN) {
        isShuttingDown.set(false)
        awaitShutdownCond.signalAll()
      }
    } finally {
      lock.unlock()
    }
    true
  }

  override def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    try {
      info("Starting broker")

      // initialize dynamic broker configs from static config. Any updates will be
      // applied as we process the metadata log.
      config.dynamicConfig.initialize() // Currently we don't wait for catch-up on the metadata log.  TODO?

      /* start scheduler */
      kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
      kafkaScheduler.startup()

      /* register broker metrics */
      _brokerTopicStats = new BrokerTopicStats

      quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))

      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

      // Create log manager, but don't start it because we need to delay any potential unclean shutdown log recovery
      // until we catch up on the metadata log and have up-to-date topic and broker configs.
      logManager = LogManager(config, initialOfflineDirs, kafkaScheduler, time,
        brokerTopicStats, logDirFailureChannel)

      metadataCache = new MetadataCache(config.brokerId)
      // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
      // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

      // Create and start the socket server acceptor threads so that the bound port is known.
      // Delay starting processors until the end of the initialization sequence to ensure
      // that credentials have been loaded before processing authentications.
      socketServer = new SocketServer(config, metrics, time, credentialProvider, Some(config.brokerId),
        Some(new LogContext(s"[SocketServer brokerId=${config.controllerId}] ")), allowDisabledApis = true)
      socketServer.startup(startProcessingRequests = false)

      // Create replica manager.
      val alterIsrManager = new AlterIsrManagerImpl(alterIsrChannelManager, kafkaScheduler,
        time, config.brokerId, () => lifecycleManager.brokerEpoch())
      // explicitly declare to eliminate spotbugs error in Scala 2.12
      this.replicaManager = new ReplicaManager(config, metrics, time, None,
        kafkaScheduler, logManager, isShuttingDown, quotaManagers,
        brokerTopicStats, metadataCache, logDirFailureChannel, alterIsrManager,
        configRepository, threadNamePrefix)

      /* start broker-to-controller channel managers */
      val controllerNodes =
        RaftConfig.quorumVoterStringsToNodes(controllerQuorumVotersFuture.get()).asScala
      val controllerNodeProvider = new RaftControllerNodeProvider(metaLogManager, controllerNodes)
      alterIsrChannelManager = BrokerToControllerChannelManager(controllerNodeProvider,
        time, metrics, config, 60000,"alterisr", threadNamePrefix)
      alterIsrChannelManager.start()
      forwardingChannelManager = BrokerToControllerChannelManager(controllerNodeProvider,
        time, metrics, config, 60000, "forwarding", threadNamePrefix)
      forwardingChannelManager.start()
      val forwardingManager = new ForwardingManager(forwardingChannelManager, time,
        config.requestTimeoutMs.longValue, logContext)

      /* start token manager */
      if (config.tokenAuthEnabled) {
        throw new UnsupportedOperationException("Delegation tokens are not supported")
      }
      tokenManager = new DelegationTokenManager(config, tokenCache, time , null)
      tokenManager.startup() // does nothing, we just need a token manager in order to compile right now...

      // Create group coordinator, but don't start it until we've started replica manager.
      // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
      groupCoordinator = GroupCoordinator(config, replicaManager, Time.SYSTEM, metrics)

      // Create transaction coordinator, but don't start it until we've started replica manager.
      // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
      transactionCoordinator = TransactionCoordinator(config, replicaManager,
        new KafkaScheduler(threads = 1, threadNamePrefix = "transaction-log-manager-"),
        createTemporaryProducerIdManager(), metrics, metadataCache, Time.SYSTEM)

      /* Add all reconfigurables for config change notification before starting the metadata listener */
      config.dynamicConfig.addReconfigurables(this)

      brokerMetadataListener = new BrokerMetadataListener(
        config.brokerId,
        time,
        metadataCache,
        configRepository,
        groupCoordinator,
        quotaManagers,
        replicaManager,
        transactionCoordinator,
        logManager,
        threadNamePrefix)

      val networkListeners = new ListenerCollection()
      config.advertisedListeners.foreach { ep =>
        networkListeners.add(new Listener().
          setHost(ep.host).
          setName(ep.listenerName.value()).
          setPort(socketServer.boundPort(ep.listenerName)).
          setSecurityProtocol(ep.securityProtocol.id))
      }
      lifecycleManager.start(() => brokerMetadataListener.highestMetadataOffset(),
        BrokerToControllerChannelManager(controllerNodeProvider, time, metrics, config,
          config.brokerSessionTimeoutMs.toLong, "heartbeat", threadNamePrefix),
        metaProps.clusterId, networkListeners, supportedFeatures)

      // Register a listener with the Raft layer to receive metadata event notifications
      metaLogManager.register(brokerMetadataListener)

      val endpoints = new util.ArrayList[Endpoint](networkListeners.size())
      var interBrokerListener: Endpoint = null
      networkListeners.iterator().forEachRemaining(listener => {
        val endPoint = new Endpoint(listener.name(),
          SecurityProtocol.forId(listener.securityProtocol()),
          listener.host(), listener.port())
        endpoints.add(endPoint)
        if (listener.name().equals(config.interBrokerListenerName.value())) {
          interBrokerListener = endPoint
        }
      })
      if (interBrokerListener == null) {
        throw new RuntimeException("Unable to find inter-broker listener " +
          config.interBrokerListenerName.value() + ". Found listener(s): " +
          endpoints.asScala.map(ep => ep.listenerName().orElse("(none)")).mkString(", "))
      }
      val authorizerInfo = KafkaAuthorizerServerInfo(new ClusterResource(clusterId),
        config.brokerId, endpoints, interBrokerListener)

      /* Get the authorizer and initialize it if one is specified.*/
      authorizer = config.authorizer
      authorizer.foreach(_.configure(config.originals))
      val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
        case Some(authZ) =>
          authZ.start(authorizerInfo).asScala.map { case (ep, cs) =>
            ep -> cs.toCompletableFuture
          }
        case None =>
          authorizerInfo.endpoints.asScala.map { ep =>
            ep -> CompletableFuture.completedFuture[Void](null)
          }.toMap
      }

      val fetchManager = new FetchManager(Time.SYSTEM,
        new FetchSessionCache(config.maxIncrementalFetchSessionCacheSlots,
          KafkaBroker.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS))

      // Start processing requests once we've caught up on the metadata log, recovered logs if necessary,
      // and started all services that we previously delayed starting.
      dataPlaneRequestProcessor = new KafkaApis(socketServer.dataPlaneRequestChannel,
        replicaManager, null, groupCoordinator, transactionCoordinator,
        null, forwardingManager, null, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
        fetchManager, brokerTopicStats, clusterId, time, tokenManager, brokerFeatures, featureCache, configRepository)

      dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
        config.numIoThreads, s"${SocketServer.DataPlaneMetricPrefix}RequestHandlerAvgIdlePercent", SocketServer.DataPlaneThreadPrefix)

      socketServer.controlPlaneRequestChannelOpt.foreach { controlPlaneRequestChannel =>
        controlPlaneRequestProcessor = new KafkaApis(controlPlaneRequestChannel,
          replicaManager, null, groupCoordinator, transactionCoordinator,
          null, forwardingManager, null, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
          fetchManager, brokerTopicStats, clusterId, time, tokenManager, brokerFeatures, featureCache, configRepository)

        controlPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.controlPlaneRequestChannelOpt.get, controlPlaneRequestProcessor, time,
          1, s"${SocketServer.ControlPlaneMetricPrefix}RequestHandlerAvgIdlePercent", SocketServer.ControlPlaneThreadPrefix)
      }

      // Block until we've caught up on the metadata log
      lifecycleManager.initialCatchUpFuture.get()
      // Start log manager, which will perform (potentially lengthy) recovery-from-unclean-shutdown if required.
      logManager.startup()
      // Start other services that we've delayed starting, in the appropriate order.
      replicaManager.startup()
      replicaManager.startHighWatermarkCheckPointThread()
      groupCoordinator.startup(metadataCache.numPartitions(Topic.GROUP_METADATA_TOPIC_NAME).
        getOrElse(config.offsetsTopicPartitions))
      transactionCoordinator.startup(metadataCache.numPartitions(Topic.TRANSACTION_STATE_TOPIC_NAME).
        getOrElse(config.transactionTopicPartitions))

      socketServer.startProcessingRequests(authorizerFutures)

      // We're now ready to unfence the broker.
      lifecycleManager.setReadyToUnfence()

      info(KafkaBroker.STARTED_MESSAGE)
      maybeChangeStatus(STARTING, STARTED)
    } catch {
      case e: Throwable =>
        maybeChangeStatus(STARTING, STARTED)
        fatal("Fatal error during broker startup. Prepare to shutdown", e)
        shutdown()
        throw e
    }
  }

  class TemporaryProducerIdManager() extends ProducerIdGenerator {
    val maxProducerIdsPerBrokerEpoch = 1000000
    var currentOffset = -1
    override def generateProducerId(): Long = {
      currentOffset = currentOffset + 1
      if (currentOffset >= maxProducerIdsPerBrokerEpoch) {
        fatal(s"Exhausted all demo/temporary producerIds as the next one will has extend past the block size of $maxProducerIdsPerBrokerEpoch")
        throw new KafkaException("Have exhausted all demo/temporary producerIds.")
      }
      lifecycleManager.initialCatchUpFuture.get()
      lifecycleManager.brokerEpoch() * maxProducerIdsPerBrokerEpoch + currentOffset
    }
  }

  def createTemporaryProducerIdManager(): ProducerIdGenerator = {
    new TemporaryProducerIdManager()
  }

  def shutdown(): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      info("shutting down")

      if (config.controlledShutdownEnable) {
        lifecycleManager.beginControlledShutdown()
        try {
          lifecycleManager.controlledShutdownFuture.get(5L, TimeUnit.MINUTES)
        } catch {
          case _: TimeoutException =>
            error("Timed out waiting for the controller to approve controlled shutdown")
          case e: Throwable =>
            error("Got unexpected exception waiting for controlled shutdown future", e)
        }
      }
      lifecycleManager.beginShutdown()

      // Stop socket server to stop accepting any more connections and requests.
      // Socket server will be shutdown towards the end of the sequence.
      if (socketServer != null) {
        CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
      }
      if (dataPlaneRequestHandlerPool != null)
        CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
      if (controlPlaneRequestHandlerPool != null)
        CoreUtils.swallow(controlPlaneRequestHandlerPool.shutdown(), this)
      if (kafkaScheduler != null)
        CoreUtils.swallow(kafkaScheduler.shutdown(), this)

      if (dataPlaneRequestProcessor != null)
        CoreUtils.swallow(dataPlaneRequestProcessor.close(), this)
      if (controlPlaneRequestProcessor != null)
        CoreUtils.swallow(controlPlaneRequestProcessor.close(), this)
      CoreUtils.swallow(authorizer.foreach(_.close()), this)

      if (brokerMetadataListener !=  null) {
        CoreUtils.swallow(brokerMetadataListener.close(), this)
      }
      if (transactionCoordinator != null)
        CoreUtils.swallow(transactionCoordinator.shutdown(), this)
      if (groupCoordinator != null)
        CoreUtils.swallow(groupCoordinator.shutdown(), this)

      if (tokenManager != null)
        CoreUtils.swallow(tokenManager.shutdown(), this)

      if (replicaManager != null)
        CoreUtils.swallow(replicaManager.shutdown(), this)

      if (alterIsrChannelManager != null)
        CoreUtils.swallow(alterIsrChannelManager.shutdown(), this)

      if (forwardingChannelManager != null)
        CoreUtils.swallow(forwardingChannelManager.shutdown(), this)

      if (logManager != null)
        CoreUtils.swallow(logManager.shutdown(), this)

      if (quotaManagers != null)
        CoreUtils.swallow(quotaManagers.shutdown(), this)

      if (socketServer != null)
        CoreUtils.swallow(socketServer.shutdown(), this)
      if (metrics != null)
        CoreUtils.swallow(metrics.close(), this)
      if (brokerTopicStats != null)
        CoreUtils.swallow(brokerTopicStats.close(), this)

      // Clear all reconfigurable instances stored in DynamicBrokerConfig
      config.dynamicConfig.clear()

      isShuttingDown.set(false)

      CoreUtils.swallow(lifecycleManager.close(), this)

      CoreUtils.swallow(AppInfoParser.unregisterAppInfo(metricsPrefix, config.brokerId.toString, metrics), this)
      info("shut down completed")
    } catch {
      case e: Throwable =>
        fatal("Fatal error during broker shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
  }

  override def awaitShutdown(): Unit = {
    lock.lock()
    try {
      while (true) {
        if (status == SHUTDOWN) return
        awaitShutdownCond.awaitUninterruptibly()
      }
    } finally {
      lock.unlock()
    }
  }

  override def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

  override def currentState(): BrokerState = lifecycleManager.state()

}
