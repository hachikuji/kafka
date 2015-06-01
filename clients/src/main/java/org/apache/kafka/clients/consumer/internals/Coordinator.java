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
import org.apache.kafka.clients.Metadata;
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class manage the coordination process with the consumer coordinator.
 */
public final class Coordinator {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final KafkaClient client;

    private final Time time;
    private final String groupId;
    private final Metadata metadata;
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
                       Metadata metadata,
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
        this.metadata = metadata;
        this.consumerCoordinator = null;
        this.subscriptions = subscriptions;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.assignmentStrategy = assignmentStrategy;
        this.heartbeat = new Heartbeat(this.sessionTimeoutMs, time.milliseconds());
        this.sensors = new CoordinatorMetrics(metrics, metricGrpPrefix, metricTags);
    }

    public void assignPartitions(final long now) {
        if (coordinatorReady(now)) {
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

            RequestHeader header = this.client.nextRequestHeader(ApiKeys.JOIN_GROUP);
            RequestSend send = new RequestSend(this.consumerCoordinator.idString(), header, request.toStruct());

            RequestCompletionHandler completionHandler = new RequestCompletionHandler() {
                @Override
                public void onComplete(ClientResponse resp) {
                    if (resp.wasDisconnected()) {
                        handleCoordinatorDisconnect(resp);
                        return;
                    }

                    // process the response
                    JoinGroupResponse response = new JoinGroupResponse(resp.responseBody());
                    short errorCode = response.errorCode();

                    if (errorCode == Errors.NONE.code()) {
                        Coordinator.this.consumerId = response.consumerId();
                        Coordinator.this.generation = response.generationId();

                        // set the flag to refresh last committed offsets
                        subscriptions.needRefreshCommits();

                        log.debug("Joined group: {}", response);

                        // record re-assignment time
                        sensors.partitionReassignments.record(time.milliseconds() - now);

                        // update subscription partition assignment
                        subscriptions.changePartitionAssignment(response.assignedPartitions());
                    } else if (errorCode == Errors.UNKNOWN_CONSUMER_ID.code()) {
                        // reset the consumer id and retry immediately
                        Coordinator.this.consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID;
                        log.info("Attempt to join group {} failed due to unknown consumer id, resetting and retrying.",
                                groupId);
                    } else if (errorCode == Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code()
                            || errorCode == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                        // re-discover the coordinator and retry with backoff
                        coordinatorDead();
                        log.info("Attempt to join group {} failed due to obsolete coordinator information, retrying.",
                                groupId);
                    } else if (errorCode == Errors.UNKNOWN_PARTITION_ASSIGNMENT_STRATEGY.code()
                            || errorCode == Errors.INCONSISTENT_PARTITION_ASSIGNMENT_STRATEGY.code()
                            || errorCode == Errors.INVALID_SESSION_TIMEOUT.code()) {
                        // log the error and re-throw the exception
                        log.error("Attempt to join group {} failed due to: {}",
                                groupId, Errors.forCode(errorCode).exception().getMessage());
                        Errors.forCode(errorCode).maybeThrow();
                    } else {
                        // unexpected error, throw the exception
                        throw new KafkaException("Unexpected error in join group response: "
                                + Errors.forCode(response.errorCode()).exception().getMessage());
                    }
                }
            };
            this.client.send(new ClientRequest(now, true, send, completionHandler));
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions.
     *
     * A non-blocking commit will attempt to commit offsets asychronously. No error will be thrown if the commit fails.
     * A blocking commit will wait for a response acknowledging the commit. In the event of an error it will retry until
     * the commit succeeds.
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @param now The current time
     * @param success Nullable parameter to track the result of the operation
     */
    public void commitOffsets(final Map<TopicPartition, Long> offsets, long now, final AtomicBoolean success) {
        if (!offsets.isEmpty() && coordinatorReady(now)) {
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

            // send request and possibly wait for response if it is blocking
            RequestCompletionHandler handler = new CommitOffsetCompletionHandler(offsets);
            if (success != null) {
                handler = new CommitOffsetCompletionHandler(offsets) {
                    @Override
                    public void onComplete(ClientResponse resp) {
                        super.onComplete(resp);
                        success.set(true);

                        OffsetCommitResponse commitResponse = new OffsetCommitResponse(resp.responseBody());
                        for (short errorCode : commitResponse.responseData().values()) {
                            if (errorCode != Errors.NONE.code())
                                success.set(false);
                        }
                    }
                };
            }

            RequestHeader header = this.client.nextRequestHeader(ApiKeys.OFFSET_COMMIT);
            RequestSend send = new RequestSend(this.consumerCoordinator.idString(), header, req.toStruct());
            this.client.send(new ClientRequest(now, true, send, handler));
        }
    }

    public void fetchOffsets(Set<TopicPartition> partitions, long now, final OffsetFetchResult result) {
        if (coordinatorReady(now)) {
            log.debug("Fetching committed offsets for partitions: " + Utils.join(partitions, ", "));
            // construct the request
            OffsetFetchRequest request = new OffsetFetchRequest(this.groupId, new ArrayList<TopicPartition>(partitions));

            // send the request with a callback
            RequestHeader header = this.client.nextRequestHeader(ApiKeys.OFFSET_FETCH);
            RequestSend send = new RequestSend(this.consumerCoordinator.idString(), header, request.toStruct());

            RequestCompletionHandler completionHandler = new RequestCompletionHandler() {
                @Override
                public void onComplete(ClientResponse resp) {
                    if (resp.wasDisconnected()) {
                        handleCoordinatorDisconnect(resp);
                        return;
                    }

                    OffsetFetchResponse response = new OffsetFetchResponse(resp.responseBody());
                    handleOffsetResponse(response, result);
                }
            };

            this.client.send(new ClientRequest(now, true, send, completionHandler));
        }
    }

    private void handleOffsetResponse(OffsetFetchResponse response, OffsetFetchResult result) {
        // parse the response to get the offsets
        boolean offsetsReady = true;

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
                    offsetsReady = false;
                } else if (data.errorCode == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                    // re-discover the coordinator and retry
                    coordinatorDead();
                    offsetsReady = false;
                } else if (data.errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
                    // just ignore this partition
                    log.debug("Unknown topic or partition for " + tp);
                } else {
                    throw new KafkaException("Unexpected error in fetch offset response: "
                            + Errors.forCode(data.errorCode).exception().getMessage());
                }
            } else if (data.offset >= 0) {
                // record the position with the offset (-1 indicates no committed offset to fetch)
                offsets.put(tp, data.offset);
            } else {
                log.debug("No committed offset for partition " + tp);
            }
        }

        if (offsetsReady)
            result.setOffsets(offsets);
    }

    /**
     * Attempt to heartbeat the consumer coordinator if necessary, and check if the coordinator is still alive.
     *
     * @param now The current time
     */
    public void maybeHeartbeat(long now) {
        if (heartbeat.shouldHeartbeat(now) && coordinatorReady(now)) {
            HeartbeatRequest req = new HeartbeatRequest(this.groupId, this.generation, this.consumerId);

            RequestHeader header = this.client.nextRequestHeader(ApiKeys.HEARTBEAT);
            RequestSend send = new RequestSend(this.consumerCoordinator.idString(), header, req.toStruct());
            ClientRequest request = new ClientRequest(now, true, send, new HeartbeatCompletionHandler());

            this.client.send(request);
            this.heartbeat.sentHeartbeat(now);
        }
    }

    public boolean coordinatorUnknown() {
        return this.consumerCoordinator == null;
    }

    private boolean coordinatorReady(long now) {
        return !coordinatorUnknown() && this.client.ready(this.consumerCoordinator, now);
    }

    public void discoverConsumerCoordinator() {
        // initiate the consumer metadata request
        // find a node to ask about the coordinator
        Node node = this.client.leastLoadedNode(time.milliseconds());
        if (this.client.ready(node, time.milliseconds())) {
            // create a consumer metadata request
            log.debug("Issuing consumer metadata request to broker {}", node.id());

            ConsumerMetadataRequest metadataRequest = new ConsumerMetadataRequest(this.groupId);
            RequestSend send = new RequestSend(node.idString(),
                    this.client.nextRequestHeader(ApiKeys.CONSUMER_METADATA),
                    metadataRequest.toStruct());

            long now = time.milliseconds();
            RequestCompletionHandler completionHandler = new RequestCompletionHandler() {
                @Override
                public void onComplete(ClientResponse response) {
                    // parse the response to get the coordinator info if it is not disconnected,
                    // otherwise we need to request metadata update
                    if (!response.wasDisconnected()) {
                        ConsumerMetadataResponse consumerMetadataResponse = new ConsumerMetadataResponse(response.responseBody());
                        // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
                        // for the coordinator in the underlying network client layer
                        // TODO: this needs to be better handled in KAFKA-1935
                        if (consumerMetadataResponse.errorCode() == Errors.NONE.code())
                            consumerCoordinator = new Node(Integer.MAX_VALUE - consumerMetadataResponse.node().id(),
                                    consumerMetadataResponse.node().host(),
                                    consumerMetadataResponse.node().port());
                    } else {
                        metadata.requestUpdate();
                    }
                }
            };
            ClientRequest request = new ClientRequest(now, true, send, completionHandler);
            client.send(request);
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

        public CommitOffsetCompletionHandler(Map<TopicPartition, Long> offsets) {
            this.offsets = offsets;
        }

        @Override
        public void onComplete(ClientResponse resp) {
            if (resp.wasDisconnected()) {
                handleCoordinatorDisconnect(resp);
            } else {
                OffsetCommitResponse response = new OffsetCommitResponse(resp.responseBody());
                for (Map.Entry<TopicPartition, Short> entry : response.responseData().entrySet()) {
                    TopicPartition tp = entry.getKey();
                    short errorCode = entry.getValue();
                    long offset = this.offsets.get(tp);
                    if (errorCode == Errors.NONE.code()) {
                        log.debug("Committed offset {} for partition {}", offset, tp);
                        subscriptions.committed(tp, offset);
                    } else if (errorCode == Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code()
                            || errorCode == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                        coordinatorDead();
                    } else {
                        // do not need to throw the exception but just log the error
                        log.error("Error committing partition {} at offset {}: {}",
                            tp,
                            offset,
                            Errors.forCode(errorCode).exception().getMessage());
                    }
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
