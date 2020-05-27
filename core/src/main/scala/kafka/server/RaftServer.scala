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

import java.io.File
import java.net.{InetAddress, InetSocketAddress}
import java.nio.file.Files
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import joptsimple.OptionParser
import kafka.log.{Log, LogConfig, LogManager}
import kafka.network.SocketServer
import kafka.raft.{KafkaMetadataLog, KafkaNetworkChannel, KafkaRequestPurgatory}
import kafka.security.CredentialProvider
import kafka.utils.timer.SystemTimer
import kafka.utils.{CommandLineUtils, Exit, KafkaScheduler, Logging}
import org.apache.kafka.clients.{ApiVersions, ClientDnsLookup, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.raft.{ReplicatedCounter, FileBasedStateStore, KafkaRaftClient, QuorumState, RaftConfig}

import scala.jdk.CollectionConverters._

class RaftServer(val config: KafkaConfig) extends Logging {

  private val partition = new TopicPartition("__cluster_metadata", 0)
  private val time = Time.SYSTEM

  var socketServer: SocketServer = _
  var credentialProvider: CredentialProvider = _
  var tokenCache: DelegationTokenCache = _
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = _
  var scheduler: KafkaScheduler = _
  var metrics: Metrics = _

  def startup(): Unit = {
    metrics = new Metrics()
    scheduler = new KafkaScheduler(threads = 1)

    scheduler.startup()

    tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
    credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

    socketServer = new SocketServer(config, metrics, time, credentialProvider)
    socketServer.startup(startProcessingRequests = false)

    val logDirName = Log.logDirName(partition)
    val logDir = createLogDirectory(new File(config.logDirs.head), logDirName)

    val raftConfig = new RaftConfig(config.originals)
    val logContext = new LogContext(s"[Raft id=${config.brokerId}] ")
    val metadataLog = buildMetadataLog(logDir)
    val networkChannel = buildNetworkChannel(raftConfig, logContext)

    val requestHandler = new RaftRequestHandler(
      networkChannel,
      socketServer.dataPlaneRequestChannel,
      time
    )

    val raftClient = buildRaftClient(
      raftConfig,
      metadataLog,
      networkChannel,
      logContext,
      logDir
    )

    dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(
      config.brokerId,
      socketServer.dataPlaneRequestChannel,
      requestHandler,
      time,
      config.numIoThreads,
      s"${SocketServer.DataPlaneMetricPrefix}RequestHandlerAvgIdlePercent",
      SocketServer.DataPlaneThreadPrefix
    )

    socketServer.startProcessingRequests(Map.empty)

    val shutdown = new AtomicBoolean(false)
    try {
      val counter = new ReplicatedCounter(raftClient, logContext, true)
      val incrementThread = new Thread() {
        override def run(): Unit = {
          while (!shutdown.get()) {
            if (counter.isLeader) {
              counter.increment()
              Thread.sleep(500)
            }
          }
        }
      }

      raftClient.initialize(counter)
      incrementThread.start()

      while (true) {
        raftClient.poll()
      }
    } finally {
      shutdown.set(true)
      raftClient.shutdown(5000)
    }
  }

  private def buildNetworkChannel(raftConfig: RaftConfig,
                                  logContext: LogContext): KafkaNetworkChannel = {
    val netClient = buildNetworkClient(raftConfig, logContext)
    val clientId = s"Raft-${config.brokerId}"
    new KafkaNetworkChannel(time, netClient, clientId,
      raftConfig.retryBackoffMs, raftConfig.requestTimeoutMs)
  }

  private def buildMetadataLog(logDir: File): KafkaMetadataLog = {
    if (config.logDirs.size != 1) {
      throw new ConfigException("There must be exactly one configured log dir")
    }

    val defaultProps = KafkaServer.copyKafkaConfigToLog(config)
    LogConfig.validateValues(defaultProps)
    val defaultLogConfig = LogConfig(defaultProps)

    val log = Log(
      dir = logDir,
      config = defaultLogConfig,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = scheduler,
      brokerTopicStats = new BrokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = config.transactionalIdExpirationMs,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(5)
    )
    new KafkaMetadataLog(time, log)
  }

  private def createLogDirectory(logDir: File, logDirName: String): File = {
    val logDirPath = logDir.getAbsolutePath
    val dir = new File(logDirPath, logDirName)
    Files.createDirectories(dir.toPath)
    dir
  }

  private def buildRaftClient(raftConfig: RaftConfig,
                              metadataLog: KafkaMetadataLog,
                              networkChannel: KafkaNetworkChannel,
                              logContext: LogContext,
                              logDir: File): KafkaRaftClient = {
    val quorumState = new QuorumState(
      config.brokerId,
      raftConfig.bootstrapVoters,
      new FileBasedStateStore(new File(logDir, "quorum-state")),
      logContext
    )

    val purgatory = new KafkaRequestPurgatory(
      config.brokerId,
      new SystemTimer("raft-purgatory-reaper"),
      reaperEnabled = true)

    new KafkaRaftClient(
      raftConfig,
      networkChannel,
      metadataLog,
      quorumState,
      time,
      purgatory,
      advertisedListener,
      logContext
    )
  }

  private def buildNetworkClient(raftConfig: RaftConfig,
                                 logContext: LogContext): NetworkClient = {
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      config.interBrokerSecurityProtocol,
      JaasContext.Type.SERVER,
      config,
      config.interBrokerListenerName,
      config.saslMechanismInterBrokerProtocol,
      time,
      config.saslInterBrokerHandshakeRequestEnable,
      logContext
    )
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      config.connectionsMaxIdleMs,
      metrics,
      time,
      "raft-channel",
      Map.empty[String, String].asJava,
      false,
      channelBuilder,
      logContext
    )
    new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      s"broker-${config.brokerId}-raft-client",
      1,
      50,
      50,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.socketReceiveBufferBytes,
      raftConfig.requestTimeoutMs,
      ClientDnsLookup.USE_ALL_DNS_IPS,
      time,
      false,
      new ApiVersions,
      logContext
    )
  }

  private def advertisedListener: InetSocketAddress = {
    val host = Option(config.advertisedListeners
      .find(_.listenerName == config.interBrokerListenerName).head.host)
      .getOrElse(InetAddress.getLocalHost.getCanonicalHostName)
    val port = socketServer.boundPort(config.interBrokerListenerName)
    new InetSocketAddress(host, port)
  }

}

object RaftServer extends Logging {
  import kafka.utils.Implicits._

  def getPropsFromArgs(args: Array[String]): Properties = {
    val optionParser = new OptionParser(false)
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])
    // This is just to make the parameter show up in the help output, we are not actually using this due the
    // fact that this class ignores the first parameter which is interpreted as positional and mandatory
    // but would not be mandatory if --version is specified
    // This is a bit of an ugly crutch till we get a chance to rework the entire command line parsing
    optionParser.accepts("version", "Print version information and exit.")

    if (args.length == 0 || args.contains("--help")) {
      CommandLineUtils.printUsageAndDie(optionParser, "USAGE: java [options] %s server.properties [--override property=value]*".format(classOf[RaftServer].getSimpleName()))
    }

    if (args.contains("--version")) {
      CommandLineUtils.printVersionAndDie()
    }

    val props = Utils.loadProps(args(0))

    if (args.length > 1) {
      val options = optionParser.parse(args.slice(1, args.length): _*)

      if (options.nonOptionArguments().size() > 0) {
        CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
      }

      props ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt).asScala)
    }
    props
  }

  def main(args: Array[String]): Unit = {
    try {
      val serverProps = getPropsFromArgs(args)
      val config = KafkaConfig.fromProps(serverProps, false)
      val server = new RaftServer(config)
      server.startup()
    }
    catch {
      case e: Throwable =>
        fatal("Exiting Kafka due to fatal exception", e)
        Exit.exit(1)
    }
    Exit.exit(0)
  }
}
