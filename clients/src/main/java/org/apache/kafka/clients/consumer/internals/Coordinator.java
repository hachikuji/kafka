/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.ConsumerMetadataRequest;
import org.apache.kafka.common.requests.ConsumerMetadataResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This class manage the coordination process with the consumer coordinator.
 */
public final class Coordinator {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final KafkaClient client;

    private final Time time;
    private final String groupId;
    private final Heartbeat heartbeat;
    private final long sessionTimeoutMs;
    private final String assignmentStrategy;
    private final SubscriptionState subscriptions;
    private final CoordinatorMetrics sensors;
    private Node consumerCoordinator;
    private String consumerId;
    private int generation;

    /**
     * Initialize the coordination manager.
     */
    public Coordinator(KafkaClient client,
                       String groupId,
                       long sessionTimeoutMs,
                       String assignmentStrategy,
                       SubscriptionState subscriptions,
                       Metrics metrics,
                       String metricGrpPrefix,
                       Map<String, String> metricTags,
                       Time time) {

        this.time = time;
        this.client = client;
        this.generation = -1;
        this.consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID;
        this.groupId = groupId;
        this.consumerCoordinator = null;
        this.subscriptions = subscriptions;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.assignmentStrategy = assignmentStrategy;
        this.heartbeat = new Heartbeat(this.sessionTimeoutMs, time.milliseconds());
        this.sensors = new CoordinatorMetrics(metrics, metricGrpPrefix, metricTags);
    }

    /**
     * Get a new partition assignment. This will send a JoinGroup request to the coordinator (if it
     * is available), which will update subscription assignments if it is completed successfully.
     * @param now The current time in milliseconds
     * @return A delayed response whose completion indicates the result of the JoinGroup request.
     */
    public CoordinatorResult<Void> assignPartitions(final long now) {
        final CoordinatorResult<Void> result = newCoordinatorResult(now);
        if (result.isReady()) return result;

        // send a join group request to the coordinator
        List<String> subscribedTopics = new ArrayList<String>(subscriptions.subscribedTopics());
        log.debug("(Re-)joining group {} with subscribed topics {}", groupId, subscribedTopics);

        JoinGroupRequest request = new JoinGroupRequest(groupId,
                (int) this.sessionTimeoutMs,
                subscribedTopics,
                this.consumerId,
                this.assignmentStrategy);

        // create the request for the coordinator
        log.debug("Issuing request ({}: {}) to coordinator {}", ApiKeys.JOIN_GROUP, request, this.consumerCoordinator.id());

        RequestCompletionHandler completionHandler = new RequestCompletionHandler() {
            @Override
            public void onComplete(ClientResponse resp) {
                handleJoinResponse(resp, result, now);
            }
        };

        sendCoordinator(ApiKeys.JOIN_GROUP, request.toStruct(), completionHandler, now);
        return result;
    }

    private void handleJoinResponse(ClientResponse response, CoordinatorResult<Void> result, long requestTime) {
        if (response.wasDisconnected()) {
            handleCoordinatorDisconnect(response);
            result.needNewCoordinator();
        } else {
            // process the response
            JoinGroupResponse joinResponse = new JoinGroupResponse(response.responseBody());
            short errorCode = joinResponse.errorCode();

            if (errorCode == Errors.NONE.code()) {
                Coordinator.this.consumerId = joinResponse.consumerId();
                Coordinator.this.generation = joinResponse.generationId();

                // set the flag to refresh last committed offsets
                subscriptions.needRefreshCommits();

                log.debug("Joined group: {}", response);

                // record re-assignment time
                this.sensors.partitionReassignments.record(time.milliseconds() - requestTime);

                // update partition assignment
                subscriptions.changePartitionAssignment(joinResponse.assignedPartitions());
                result.complete(null);
            } else if (errorCode == Errors.UNKNOWN_CONSUMER_ID.code()) {
                // reset the consumer id and retry immediately
                Coordinator.this.consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID;
                log.info("Attempt to join group {} failed due to unknown consumer id, resetting and retrying.",
                        groupId);

                result.needRetry();
            } else if (errorCode == Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code()
                    || errorCode == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                // re-discover the coordinator and retry with backoff
                coordinatorDead();
                log.info("Attempt to join group {} failed due to obsolete coordinator information, retrying.",
                        groupId);
                result.needNewCoordinator();
            } else if (errorCode == Errors.UNKNOWN_PARTITION_ASSIGNMENT_STRATEGY.code()
                    || errorCode == Errors.INCONSISTENT_PARTITION_ASSIGNMENT_STRATEGY.code()
                    || errorCode == Errors.INVALID_SESSION_TIMEOUT.code()) {
                // log the error and re-throw the exception
                KafkaException e = Errors.forCode(errorCode).exception();
                log.error("Attempt to join group {} failed due to: {}",
                        groupId, e.getMessage());
                result.raise(e);
            } else {
                // unexpected error, throw the exception
                result.raise(new KafkaException("Unexpected error in join group response: "
                        + Errors.forCode(joinResponse.errorCode()).exception().getMessage()));
            }
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This method returns a delayed
     * response which can be polled on for the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @param now The current time
     * @return A delayed response whose value indicates whether the commit was successful or not
     */
    public CoordinatorResult<Void> commitOffsets(final Map<TopicPartition, Long> offsets, long now) {
        final CoordinatorResult<Void> result = newCoordinatorResult(now);
        if (result.isReady()) return result;

        if (offsets.isEmpty()) {
            result.complete(null);
        } else {
            // create the offset commit request
            Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData;
            offsetData = new HashMap<TopicPartition, OffsetCommitRequest.PartitionData>(offsets.size());
            for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet())
                offsetData.put(entry.getKey(), new OffsetCommitRequest.PartitionData(entry.getValue(), ""));
            OffsetCommitRequest req = new OffsetCommitRequest(this.groupId,
                this.generation,
                this.consumerId,
                OffsetCommitRequest.DEFAULT_RETENTION_TIME,
                offsetData);

            RequestCompletionHandler handler = new CommitOffsetCompletionHandler(offsets, result);
            sendCoordinator(ApiKeys.OFFSET_COMMIT, req.toStruct(), handler, now);
        }

        return result;
    }

    private <T> CoordinatorResult<T> newCoordinatorResult(long now) {
        if (coordinatorUnknown()) {
            return CoordinatorResult.newCoordinatorNeeded();
        } else if (!this.client.ready(this.consumerCoordinator, now)) {
            return CoordinatorResult.retryNeeded();
        }
        return new CoordinatorResult<T>();
    }

    /**
     * Fetch the committed offsets for a set of partitions.
     *
     * @param partitions The set of partitions to get offsets for.
     * @param now The current time in milliseconds
     * @return A delayed response containing the committed offsets.
     */
    public CoordinatorResult<Map<TopicPartition, Long>> fetchOffsets(Set<TopicPartition> partitions, long now) {
        final CoordinatorResult<Map<TopicPartition, Long>> result = newCoordinatorResult(now);
        if (result.isReady()) return result;

        log.debug("Fetching committed offsets for partitions: " + Utils.join(partitions, ", "));
        // construct the request
        OffsetFetchRequest request = new OffsetFetchRequest(this.groupId, new ArrayList<TopicPartition>(partitions));

        // send the request with a callback
        RequestCompletionHandler completionHandler = new RequestCompletionHandler() {
            @Override
            public void onComplete(ClientResponse resp) {
                handleOffsetResponse(resp, result);
            }
        };
        sendCoordinator(ApiKeys.OFFSET_FETCH, request.toStruct(), completionHandler, now);
        return result;
    }

    private void handleOffsetResponse(ClientResponse resp, CoordinatorResult<Map<TopicPartition, Long>> result) {
        if (resp.wasDisconnected()) {
            result.needRetry();
            handleCoordinatorDisconnect(resp);
        } else {
            // parse the response to get the offsets
            OffsetFetchResponse response = new OffsetFetchResponse(resp.responseBody());
            Map<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>(response.responseData().size());
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData data = entry.getValue();
                if (data.hasError()) {
                    log.debug("Error fetching offset for topic-partition {}: {}", tp, Errors.forCode(data.errorCode)
                            .exception()
                            .getMessage());
                    if (data.errorCode == Errors.OFFSET_LOAD_IN_PROGRESS.code()) {
                        // just retry
                        result.needRetry();
                    } else if (data.errorCode == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                        // re-discover the coordinator and retry
                        coordinatorDead();
                        result.needNewCoordinator();
                    } else if (data.errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
                        // just ignore this partition
                        log.debug("Unknown topic or partition for " + tp);
                    } else {
                        result.raise(new KafkaException("Unexpected error in fetch offset response: "
                                + Errors.forCode(data.errorCode).exception().getMessage()));
                    }
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    offsets.put(tp, data.offset);
                } else {
                    log.debug("No committed offset for partition " + tp);
                }
            }

            if (!result.isReady()) {
                result.complete(offsets);
            }
        }
    }

    /**
     * Attempt to heartbeat the consumer coordinator if necessary, and check if the coordinator is still alive.
     *
     * @param now The current time
     */
    public void maybeHeartbeat(long now) {
        if (heartbeat.shouldHeartbeat(now) && coordinatorReady(now)) {
            HeartbeatRequest req = new HeartbeatRequest(this.groupId, this.generation, this.consumerId);
            sendCoordinator(ApiKeys.HEARTBEAT, req.toStruct(), new HeartbeatCompletionHandler(), now);
            this.heartbeat.sentHeartbeat(now);
        }
    }

    /**
     * Get the time until the next heartbeat is needed.
     * @param now The current time
     * @return The duration in milliseconds before the next heartbeat will be needed.
     */
    public long timeToNextHeartbeat(long now) {
        return heartbeat.timeToNextHeartbeat(now);
    }

    public boolean coordinatorUnknown() {
        return this.consumerCoordinator == null;
    }

    private boolean coordinatorReady(long now) {
        return !coordinatorUnknown() && this.client.ready(this.consumerCoordinator, now);
    }

    /**
     * Discover the current coordinator for the consumer group. Sends a ConsumerMetadata request to
     * one of the brokers.
     * @return A delayed response which indicates the completion of the metadata request
     */
    public BrokerResult<Void> discoverConsumerCoordinator() {
        // initiate the consumer metadata request
        // find a node to ask about the coordinator
        long now = time.milliseconds();
        Node node = this.client.leastLoadedNode(now);

        if (!this.client.ready(node, now)) {
            return BrokerResult.retryNeeded();
        } else {
            final BrokerResult<Void> result = new BrokerResult<Void>();

            // create a consumer metadata request
            log.debug("Issuing consumer metadata request to broker {}", node.id());
            ConsumerMetadataRequest metadataRequest = new ConsumerMetadataRequest(this.groupId);
            RequestCompletionHandler completionHandler = new RequestCompletionHandler() {
                @Override
                public void onComplete(ClientResponse resp) {
                    handleConsumerMetadataResponse(resp, result);
                }
            };
            send(node, ApiKeys.CONSUMER_METADATA, metadataRequest.toStruct(), completionHandler, now);
            return result;
        }
    }

    private void handleConsumerMetadataResponse(ClientResponse resp, BrokerResult<Void> result) {
        log.debug("Consumer metadata response {}", resp);

        // parse the response to get the coordinator info if it is not disconnected,
        // otherwise we need to request metadata update
        if (resp.wasDisconnected()) {
            result.needMetadataRefresh();
        } else {
            ConsumerMetadataResponse consumerMetadataResponse = new ConsumerMetadataResponse(resp.responseBody());
            // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
            // for the coordinator in the underlying network client layer
            // TODO: this needs to be better handled in KAFKA-1935
            if (consumerMetadataResponse.errorCode() == Errors.NONE.code()) {
                Coordinator.this.consumerCoordinator = new Node(Integer.MAX_VALUE - consumerMetadataResponse.node().id(),
                        consumerMetadataResponse.node().host(),
                        consumerMetadataResponse.node().port());
                result.complete(null);
            } else {
                result.needRetry();
            }
        }
    }

    /**
     * Mark the current coordinator as dead.
     */
    private void coordinatorDead() {
        if (this.consumerCoordinator != null) {
            log.info("Marking the coordinator {} dead.", this.consumerCoordinator.id());
            this.consumerCoordinator = null;
        }
    }

    /**
     * Handle the case when the request gets cancelled due to coordinator disconnection.
     */
    private void handleCoordinatorDisconnect(ClientResponse response) {
        int correlation = response.request().request().header().correlationId();
        log.debug("Cancelled request {} with correlation id {} due to coordinator {} being disconnected",
                response.request(),
                correlation,
                response.request().request().destination());

        // mark the coordinator as dead
        coordinatorDead();
    }


    private void sendCoordinator(ApiKeys api, Struct request, RequestCompletionHandler handler, long now) {
        send(this.consumerCoordinator, api, request, handler, now);
    }

    private void send(Node node, ApiKeys api, Struct request, RequestCompletionHandler handler, long now) {
        RequestHeader header = this.client.nextRequestHeader(api);
        RequestSend send = new RequestSend(node.idString(), header, request);
        this.client.send(new ClientRequest(now, true, send, handler));
    }

    private class HeartbeatCompletionHandler implements RequestCompletionHandler {
        @Override
        public void onComplete(ClientResponse resp) {
            if (resp.wasDisconnected()) {
                handleCoordinatorDisconnect(resp);
            } else {
                HeartbeatResponse response = new HeartbeatResponse(resp.responseBody());
                if (response.errorCode() == Errors.NONE.code()) {
                    log.debug("Received successful heartbeat response.");
                } else if (response.errorCode() == Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code()
                        || response.errorCode() == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                    log.info("Attempt to heart beat failed since coordinator is either not started or not valid, marking it as dead.");
                    coordinatorDead();
                } else if (response.errorCode() == Errors.ILLEGAL_GENERATION.code()) {
                    log.info("Attempt to heart beat failed since generation id is not legal, try to re-join group.");
                    subscriptions.needReassignment();
                } else if (response.errorCode() == Errors.UNKNOWN_CONSUMER_ID.code()) {
                    log.info("Attempt to heart beat failed since consumer id is not valid, reset it and try to re-join group.");
                    consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID;
                    subscriptions.needReassignment();
                } else {
                    throw new KafkaException("Unexpected error in heartbeat response: "
                        + Errors.forCode(response.errorCode()).exception().getMessage());
                }
            }
            sensors.heartbeatLatency.record(resp.requestLatencyMs());
        }
    }

    private class CommitOffsetCompletionHandler implements RequestCompletionHandler {

        private final Map<TopicPartition, Long> offsets;
        private final CoordinatorResult<Void> result;

        public CommitOffsetCompletionHandler(Map<TopicPartition, Long> offsets, CoordinatorResult<Void> result) {
            this.offsets = offsets;
            this.result = result;
        }

        @Override
        public void onComplete(ClientResponse resp) {
            if (resp.wasDisconnected()) {
                handleCoordinatorDisconnect(resp);
                result.needNewCoordinator();
            } else {
                OffsetCommitResponse commitResponse = new OffsetCommitResponse(resp.responseBody());
                for (Map.Entry<TopicPartition, Short> entry : commitResponse.responseData().entrySet()) {
                    TopicPartition tp = entry.getKey();
                    short errorCode = entry.getValue();
                    long offset = this.offsets.get(tp);
                    if (errorCode == Errors.NONE.code()) {
                        log.debug("Committed offset {} for partition {}", offset, tp);
                        subscriptions.committed(tp, offset);
                    } else if (errorCode == Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code()
                            || errorCode == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                        coordinatorDead();
                        result.needNewCoordinator();
                    } else {
                        // do not need to throw the exception but just log the error
                        result.needRetry();
                        log.error("Error committing partition {} at offset {}: {}",
                            tp,
                            offset,
                            Errors.forCode(errorCode).exception().getMessage());
                    }
                }

                if (!result.isReady()) {
                    result.complete(null);
                }
            }
            sensors.commitLatency.record(resp.requestLatencyMs());
        }
    }

    private class CoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor commitLatency;
        public final Sensor heartbeatLatency;
        public final Sensor partitionReassignments;

        public CoordinatorMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> tags) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(new MetricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request",
                tags), new Avg());
            this.commitLatency.add(new MetricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request",
                tags), new Max());
            this.commitLatency.add(new MetricName("commit-rate",
                this.metricGrpName,
                "The number of commit calls per second",
                tags), new Rate(new Count()));

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(new MetricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a hearbeat request",
                tags), new Max());
            this.heartbeatLatency.add(new MetricName("heartbeat-rate",
                this.metricGrpName,
                "The average number of heartbeats per second",
                tags), new Rate(new Count()));

            this.partitionReassignments = metrics.sensor("reassignment-latency");
            this.partitionReassignments.add(new MetricName("reassignment-time-avg",
                this.metricGrpName,
                "The average time taken for a partition reassignment",
                tags), new Avg());
            this.partitionReassignments.add(new MetricName("reassignment-time-max",
                this.metricGrpName,
                "The max time taken for a partition reassignment",
                tags), new Avg());
            this.partitionReassignments.add(new MetricName("reassignment-rate",
                this.metricGrpName,
                "The number of partition reassignments per second",
                tags), new Rate(new Count()));

            Measurable numParts =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return subscriptions.assignedPartitions().size();
                    }
                };
            metrics.addMetric(new MetricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer",
                tags),
                numParts);

            Measurable lastHeartbeat =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
                    }
                };
            metrics.addMetric(new MetricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last controller heartbeat",
                tags),
                lastHeartbeat);
        }
    }
}
