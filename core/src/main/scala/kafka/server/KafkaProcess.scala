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

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.metrics.{JmxReporter, KafkaMetricsContext, MetricConfig, Metrics, MetricsReporter, Sensor}
import org.apache.kafka.common.utils.Time

/**
 * Generic trait representing the entry point for the Kafka process.
 */
trait KafkaProcess {
  def startup(): Unit
  def shutdown(): Unit
  def awaitShutdown(): Unit
}

object KafkaProcess {

  /**
   * The "legacy" zookeeper-based process implements [[KafkaProcess]]
   * through [[KafkaServer]] (whose name we retain for metric compatibility).
   */
  type KafkaZkProcess = KafkaServer

  def initializeMetrics(
    config: KafkaConfig,
    time: Time,
    clusterId: String
  ): Metrics = {
    val jmxReporter = new JmxReporter()
    jmxReporter.configure(config.originals)

    val reporters = new java.util.ArrayList[MetricsReporter]
    reporters.add(jmxReporter)

    val metricConfig = buildMetricsConfig(config)
    val metricsContext = createKafkaMetricsContext(clusterId, config)
    new Metrics(metricConfig, reporters, time, true, metricsContext)
  }

  private def buildMetricsConfig(kafkaConfig: KafkaConfig): MetricConfig = {
    new MetricConfig()
      .samples(kafkaConfig.metricNumSamples)
      .recordLevel(Sensor.RecordingLevel.forName(kafkaConfig.metricRecordingLevel))
      .timeWindow(kafkaConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS)
  }

  val MetricsPrefix: String = "kafka.server" // TODO: Probably pull this out?
  private val ClusterIdLabel: String = "kafka.cluster.id"
  private val BrokerIdLabel: String = "kafka.broker.id"

  private[server] def createKafkaMetricsContext(
    clusterId: String,
    config: KafkaConfig
  ): KafkaMetricsContext = {
    val contextLabels = new java.util.HashMap[String, Object]
    contextLabels.put(ClusterIdLabel, clusterId)
    contextLabels.put(BrokerIdLabel, config.brokerId.toString)
    contextLabels.putAll(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    val metricsContext = new KafkaMetricsContext(MetricsPrefix, contextLabels)
    metricsContext
  }
}
