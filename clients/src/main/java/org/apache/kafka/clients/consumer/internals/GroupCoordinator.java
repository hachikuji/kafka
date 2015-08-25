/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
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
import org.apache.kafka.common.requests.GroupMetadataRequest;
import org.apache.kafka.common.requests.GroupMetadataResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * GroupCoordinator implements group management for a single group member by interacting with
 * a designated Kafka broker (the coordinator). Group semantics are provided by an implementation
 * of {@link GroupController}.
 * @param <T> Protocol type used by this coordinator
 */
public class GroupCoordinator<T extends GroupProtocol> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final GroupController<T> controller;
    private final Heartbeat heartbeat;
    private final HeartbeatTask heartbeatTask;
    private final int sessionTimeoutMs;
    private final GroupCoordinatorMetrics sensors;
    protected final String groupId;
    protected final ConsumerNetworkClient client;
    protected final Time time;
    protected final long retryBackoffMs;
    protected final long requestTimeoutMs;

    private boolean rejoinNeeded = true;
    private T protocol;
    private Map<String, ByteBuffer> groupMetadata;

    protected Node coordinator;
    protected String memberId;
    protected int generation;

    /**
     * Initialize the coordination manager.
     */
    public GroupCoordinator(GroupController<T> controller,
                            ConsumerNetworkClient client,
                            String groupId,
                            int sessionTimeoutMs,
                            int heartbeatIntervalMs,
                            Metrics metrics,
                            String metricGrpPrefix,
                            Map<String, String> metricTags,
                            Time time,
                            long requestTimeoutMs,
                            long retryBackoffMs) {
        this.controller = controller;
        this.client = client;
        this.time = time;
        this.generation = -1;
        this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        this.groupId = groupId;
        this.coordinator = null;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.heartbeat = new Heartbeat(this.sessionTimeoutMs, heartbeatIntervalMs, time.milliseconds());
        this.heartbeatTask = new HeartbeatTask();
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix, metricTags);
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Block until the coordinator for this group is known.
     */
    public void ensureCoordinatorKnown() {
        while (coordinatorUnknown()) {
            RequestFuture<Void> future = sendGroupMetadataRequest();
            client.poll(future, requestTimeoutMs);

            if (future.failed())
                client.awaitMetadataUpdate();
        }
        controller.onCoordinatorFound(coordinator);
    }

    public boolean needRejoin() {
        return rejoinNeeded || controller.needRejoin();
    }

    /**
     * Ensure that we have a valid partition assignment from the coordinator.
     */
    public void ensureActiveGroup() {
        if (!needRejoin())
            return;

        if (generation > 0)
            // onLeave only invoked if we have a valid current generation
            controller.onLeave(protocol, memberId, groupMetadata);

        while (needRejoin()) {
            ensureCoordinatorKnown();
            client.ensureFreshMetadata();

            // ensure that there are no pending requests to the coordinator. This is important
            // in particular to avoid resending a pending JoinGroup request.
            if (client.pendingRequestCount(this.coordinator) > 0) {
                client.awaitPendingRequests(this.coordinator);
                continue;
            }

            RequestFuture<Void> future = sendJoinGroupRequest();
            client.poll(future);

            if (future.succeeded()) {
                controller.onJoin(protocol, memberId, groupMetadata);
            } else {
                if (!future.isRetriable())
                    throw future.exception();
                Utils.sleep(retryBackoffMs);
            }
        }
    }

    private class HeartbeatTask implements DelayedTask {

        public void reset() {
            // start or restart the heartbeat task to be executed at the next chance
            long now = time.milliseconds();
            heartbeat.resetSessionTimeout(now);
            client.unschedule(this);
            client.schedule(this, now);
        }

        @Override
        public void run(final long now) {
            if (needRejoin() || coordinatorUnknown()) {
                // no need to send the heartbeat we're not using auto-assignment or if we are
                // awaiting a rebalance
                return;
            }

            if (heartbeat.sessionTimeoutExpired(now)) {
                // we haven't received a successful heartbeat in one session interval
                // so mark the coordinator dead
                coordinatorDead();
                return;
            }

            if (!heartbeat.shouldHeartbeat(now)) {
                // we don't need to heartbeat now, so reschedule for when we do
                client.schedule(this, now + heartbeat.timeToNextHeartbeat(now));
            } else {
                heartbeat.sentHeartbeat(now);
                RequestFuture<Void> future = sendHeartbeatRequest();
                future.addListener(new RequestFutureListener<Void>() {
                    @Override
                    public void onSuccess(Void value) {
                        long now = time.milliseconds();
                        heartbeat.receiveHeartbeat(now);
                        long nextHeartbeatTime = now + heartbeat.timeToNextHeartbeat(now);
                        client.schedule(HeartbeatTask.this, nextHeartbeatTime);
                    }

                    @Override
                    public void onFailure(RuntimeException e) {
                        client.schedule(HeartbeatTask.this, time.milliseconds() + retryBackoffMs);
                    }
                });
            }
        }
    }


    /**
     * Send a request to get a new partition assignment. This is a non-blocking call which sends
     * a JoinGroup request to the coordinator (if it is available). The returned future must
     * be polled to see if the request completed successfully.
     * @return A request future whose completion indicates the result of the JoinGroup request.
     */
    private RequestFuture<Void> sendJoinGroupRequest() {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // send a join group request to the coordinator
        log.debug("(Re-)joining {} group {}", controller.protocolType(), groupId);

        List<T> protocols = controller.protocols();
        if (protocols.isEmpty())
            throw new IllegalStateException("Supported protocol list cannot be empty");

        List<JoinGroupRequest.ProtocolMetadata> protocolMetadata = new ArrayList<>();
        for (GroupProtocol protocol : protocols)
            protocolMetadata.add(new JoinGroupRequest.ProtocolMetadata(protocol.name(), protocol.version(), protocol.metadata()));

        JoinGroupRequest request = new JoinGroupRequest(
                controller.protocolType(),
                groupId,
                this.sessionTimeoutMs,
                this.memberId,
                protocolMetadata);

        // create the request for the coordinator
        log.debug("Issuing request ({}: {}) to coordinator {}", ApiKeys.JOIN_GROUP, request, this.coordinator.id());
        return client.send(coordinator, ApiKeys.JOIN_GROUP, request)
                .compose(new JoinGroupResponseHandler(protocols));
    }

    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, Void> {

        private List<T> requestProtocols;

        public JoinGroupResponseHandler(List<T> protocolMetadata) {
            this.requestProtocols = protocolMetadata;
        }

        @Override
        public JoinGroupResponse parse(ClientResponse response) {
            return new JoinGroupResponse(response.responseBody());
        }

        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<Void> future) {
            // process the response
            short errorCode = joinResponse.errorCode();

            if (errorCode == Errors.NONE.code()) {
                for (T requestProtocol : requestProtocols) {
                    if (requestProtocol.name().equals(joinResponse.groupProtocol()) &&
                            requestProtocol.version() == joinResponse.groupProtocolVersion()) {
                        GroupCoordinator.this.memberId = joinResponse.memberId();
                        GroupCoordinator.this.generation = joinResponse.generationId();
                        GroupCoordinator.this.protocol = requestProtocol;
                        GroupCoordinator.this.groupMetadata = joinResponse.groupMembers();
                        GroupCoordinator.this.rejoinNeeded = false;
                        heartbeatTask.reset();
                        future.complete(null);
                        return;
                    }
                }
                future.raise(new IllegalStateException("Coordinator selected unsupported protocol"));
            } else if (errorCode == Errors.UNKNOWN_MEMBER_ID.code()) {
                // reset the consumer id and retry immediately
                GroupCoordinator.this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                log.info("Attempt to join group {} failed due to unknown consumer id, resetting and retrying.",
                        groupId);
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (errorCode == Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code()
                    || errorCode == Errors.NOT_COORDINATOR_FOR_GROUP.code()) {
                // re-discover the coordinator and retry with backoff
                coordinatorDead();
                log.info("Attempt to join group {} failed due to obsolete coordinator information, retrying.",
                        groupId);
                future.raise(Errors.forCode(errorCode));
            } else if (errorCode == Errors.INCONSISTENT_GROUP_PROTOCOL.code()
                    || errorCode == Errors.INVALID_SESSION_TIMEOUT.code()) {
                // log the error and re-throw the exception
                Errors error = Errors.forCode(errorCode);
                log.error("Attempt to join group {} failed due to: {}",
                        groupId, error.exception().getMessage());
                future.raise(error);
            } else {
                // unexpected error, throw the exception
                future.raise(new KafkaException("Unexpected error in join group response: "
                        + Errors.forCode(joinResponse.errorCode()).exception().getMessage()));
            }
        }
    }

    /**
     * Send a heartbeat request now (visible only for testing).
     */
    public RequestFuture<Void> sendHeartbeatRequest() {
        HeartbeatRequest req = new HeartbeatRequest(this.groupId, this.generation, this.memberId);
        return client.send(coordinator, ApiKeys.HEARTBEAT, req)
                .compose(new HeartbeatCompletionHandler());
    }

    public boolean coordinatorUnknown() {
        return this.coordinator == null;
    }

    /**
     * Discover the current coordinator for the consumer group. Sends a ConsumerMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendGroupMetadataRequest() {
        // initiate the consumer metadata request
        // find a node to ask about the coordinator
        Node node = this.client.leastLoadedNode();
        if (node == null) {
            // TODO: If there are no brokers left, perhaps we should use the bootstrap set
            // from configuration?
            return RequestFuture.noBrokersAvailable();
        } else {
            // create a consumer metadata request
            log.debug("Issuing consumer metadata request to broker {}", node.id());
            GroupMetadataRequest metadataRequest = new GroupMetadataRequest(this.groupId);
            return client.send(node, ApiKeys.CONSUMER_METADATA, metadataRequest)
                    .compose(new RequestFutureAdapter<ClientResponse, Void>() {
                        @Override
                        public void onSuccess(ClientResponse response, RequestFuture<Void> future) {
                            handleGroupMetadataResponse(response, future);
                        }
                    });
        }
    }

    private void handleGroupMetadataResponse(ClientResponse resp, RequestFuture<Void> future) {
        log.debug("Consumer metadata response {}", resp);

        // parse the response to get the coordinator info if it is not disconnected,
        // otherwise we need to request metadata update
        if (resp.wasDisconnected()) {
            future.raise(new DisconnectException());
        } else if (!coordinatorUnknown()) {
            // We already found the coordinator, so ignore the request
            future.complete(null);
        } else {
            GroupMetadataResponse groupMetadataResponse = new GroupMetadataResponse(resp.responseBody());
            // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
            // for the coordinator in the underlying network client layer
            // TODO: this needs to be better handled in KAFKA-1935
            if (groupMetadataResponse.errorCode() == Errors.NONE.code()) {
                this.coordinator = new Node(Integer.MAX_VALUE - groupMetadataResponse.node().id(),
                        groupMetadataResponse.node().host(),
                        groupMetadataResponse.node().port());
                heartbeatTask.reset();
                future.complete(null);
            } else {
                future.raise(Errors.forCode(groupMetadataResponse.errorCode()));
            }
        }
    }

    /**
     * Mark the current coordinator as dead.
     */
    protected void coordinatorDead() {
        if (this.coordinator != null) {
            log.info("Marking the coordinator {} dead.", this.coordinator.id());
            this.controller.onCoordinatorDead(coordinator);
            this.coordinator = null;
        }
    }

    private class HeartbeatCompletionHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        @Override
        public HeartbeatResponse parse(ClientResponse response) {
            return new HeartbeatResponse(response.responseBody());
        }

        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatLatency.record(response.requestLatencyMs());
            short error = heartbeatResponse.errorCode();
            if (error == Errors.NONE.code()) {
                log.debug("Received successful heartbeat response.");
                future.complete(null);
            } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code()
                    || error == Errors.NOT_COORDINATOR_FOR_GROUP.code()) {
                log.info("Attempt to heart beat failed since coordinator is either not started or not valid, marking it as dead.");
                coordinatorDead();
                future.raise(Errors.forCode(error));
            } else if (error == Errors.ILLEGAL_GENERATION.code()) {
                log.info("Attempt to heart beat failed since generation id is not legal, try to re-join group.");
                GroupCoordinator.this.rejoinNeeded = true;
                future.raise(Errors.ILLEGAL_GENERATION);
            } else if (error == Errors.UNKNOWN_MEMBER_ID.code()) {
                log.info("Attempt to heart beat failed since consumer id is not valid, reset it and try to re-join group.");
                memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                GroupCoordinator.this.rejoinNeeded = true;
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: "
                        + Errors.forCode(error).exception().getMessage()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T>
            extends RequestFutureAdapter<ClientResponse, T> {
        protected ClientResponse response;

        public abstract R parse(ClientResponse response);

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            this.response = clientResponse;

            if (clientResponse.wasDisconnected()) {
                int correlation = response.request().request().header().correlationId();
                log.debug("Cancelled request {} with correlation id {} due to coordinator {} being disconnected",
                        response.request(),
                        correlation,
                        response.request().request().destination());

                // mark the coordinator as dead
                coordinatorDead();
                future.raise(new DisconnectException());
                return;
            }

            R response = parse(clientResponse);
            handle(response, future);
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            if (e instanceof DisconnectException) {
                log.debug("Coordinator request failed", e);
                coordinatorDead();
            }
            future.raise(e);
        }
    }

    private class GroupCoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor heartbeatLatency;
        public final Sensor partitionReassignments;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> tags) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

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
