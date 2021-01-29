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
import java.util.concurrent._

import com.yammer.metrics.{core => yammer}
import kafka.controller.KafkaController
import kafka.log.{LogConfig, LogManager}
import kafka.metrics.{KafkaMetricsGroup, KafkaYammerMetrics, LinuxIoMetricsCollector}
import kafka.network.SocketServer
import kafka.utils.{KafkaScheduler, Logging}
import kafka.zk.BrokerInfo
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.{KafkaMetricsContext, MetricConfig, Metrics, MetricsReporter, Sensor}
import org.apache.kafka.common.ClusterResource
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.authorizer.Authorizer

import scala.jdk.CollectionConverters._
import scala.collection.Seq

object KafkaBroker {
  //properties for MetricsContext
  val metricsPrefix: String = "kafka.server"
  private val KAFKA_CLUSTER_ID: String = "kafka.cluster.id"
  private val KAFKA_BROKER_ID: String = "kafka.broker.id"
  private val KAFKA_CONTROLLER_ID: String = "kafka.controller.id"

  private[server] def createKafkaMetricsContext(
    metaProperties: MetaProperties,
    config: KafkaConfig
  ): KafkaMetricsContext = {
    val contextLabels = new util.HashMap[String, Object]
    contextLabels.put(KAFKA_CLUSTER_ID, metaProperties.clusterId)

    metaProperties.brokerId.foreach { brokerId =>
      contextLabels.put(KAFKA_BROKER_ID, brokerId.toString)
    }

    metaProperties.controllerId.foreach { controllerId =>
      contextLabels.put(KAFKA_CONTROLLER_ID, controllerId.toString)
    }

    contextLabels.putAll(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    val metricsContext = new KafkaMetricsContext(metricsPrefix, contextLabels)
    metricsContext
  }

  private[server] def createKafkaMetricsContext(
    clusterId: String,
    config: KafkaConfig
  ): KafkaMetricsContext = {
    val contextLabels = new util.HashMap[String, Object]
    contextLabels.put(KAFKA_CLUSTER_ID, clusterId)
    contextLabels.put(KAFKA_BROKER_ID, config.brokerId.toString)
    contextLabels.putAll(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    val metricsContext = new KafkaMetricsContext(metricsPrefix, contextLabels)
    metricsContext
  }

  private[server] def notifyClusterListeners(clusterId: String,
                                             clusterListeners: Seq[AnyRef]): Unit = {
    val clusterResourceListeners = new ClusterResourceListeners
    clusterResourceListeners.maybeAddAll(clusterListeners.asJava)
    clusterResourceListeners.onUpdate(new ClusterResource(clusterId))
  }

  private[server] def notifyMetricsReporters(clusterId: String,
                                             config: KafkaConfig,
                                             metricsReporters: Seq[AnyRef]): Unit = {
    val metricsContext = KafkaBroker.createKafkaMetricsContext(clusterId, config)
    metricsReporters.foreach {
      case x: MetricsReporter => x.contextChange(metricsContext)
      case _ => //do nothing
    }
  }

  /**
   * The log message that we print when the broker has been successfully started.
   * The ducktape system tests look for a line matching the regex 'Kafka\s*Server.*started'
   * to know when the broker is started, so it is best not to change this message -- but if
   * you do change it, be sure to make it match that regex or the system tests will fail.
   */
  val STARTED_MESSAGE = "Kafka Server started"

  val MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS: Long = 120000
}

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
trait KafkaBroker extends Logging with KafkaMetricsGroup {
  def startup(): Unit
  def shutdown(): Unit
  def awaitShutdown(): Unit
  def boundPort(listenerName: ListenerName): Int
  def metrics(): Metrics
  def currentState(): BrokerState
  def clusterId(): String

  // methods required for legacy/kip500-agnostic dynamic configuration
  def config: KafkaConfig
  def authorizer: Option[Authorizer]
  def kafkaYammerMetrics: KafkaYammerMetrics
  def quotaManagers: QuotaFactory.QuotaManagers
  def dataPlaneRequestHandlerPool: KafkaRequestHandlerPool
  def socketServer: SocketServer
  def replicaManager: ReplicaManager
  def logManager: LogManager
  def kafkaScheduler: KafkaScheduler
  def legacyController: Option[KafkaController] = None // must override in legacy broker
  def createBrokerInfo: BrokerInfo = throw new UnsupportedOperationException("Unsupported in KIP-500 mode") // must override in legacy broker

  newKafkaServerGauge("BrokerState", () => currentState().value())
  newKafkaServerGauge("ClusterId", () => clusterId())
  newKafkaServerGauge("yammer-metrics-count", () =>  KafkaYammerMetrics.defaultRegistry.allMetrics.size)

  val linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", Time.SYSTEM, logger.underlying)

  if (linuxIoMetricsCollector.usable()) {
    newGauge("linux-disk-read-bytes", () => linuxIoMetricsCollector.readBytes())
    newGauge("linux-disk-write-bytes", () => linuxIoMetricsCollector.writeBytes())
  }

  // For backwards compatibility, we need to keep older metrics tied
  // to their original name when this class was named `KafkaServer`
  private def newKafkaServerGauge[T](metricName: String, gauge: yammer.Gauge[T]): yammer.Gauge[T] = {
    val explicitName = explicitMetricName(
      group = "kafka.server",
      typeName = "KafkaServer",
      name = metricName,
      tags = Map.empty
    )
    KafkaYammerMetrics.defaultRegistry().newGauge(explicitName, gauge)
  }
}
