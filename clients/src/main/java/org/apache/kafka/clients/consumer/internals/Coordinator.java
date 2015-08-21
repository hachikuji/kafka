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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.CommitType;
import org.apache.kafka.clients.consumer.ConsumerCommitCallback;
import org.apache.kafka.clients.consumer.PartitionAssignor;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
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
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class manages the coordination process with the consumer coordinator.
 */
public final class Coordinator extends GroupCoordinator<PartitionAssignmentProtocol> {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final ConsumerCoordinatorMetrics sensors;
    private final SubscriptionState subscriptions;

    /**
     * Initialize the coordination manager.
     */
    public Coordinator(ConsumerNetworkClient client,
                       String groupId,
                       int sessionTimeoutMs,
                       int heartbeatIntervalMs,
                       List<PartitionAssignor<?>> assignors,
                       Metadata metadata,
                       SubscriptionState subscriptions,
                       Metrics metrics,
                       String metricGrpPrefix,
                       Map<String, String> metricTags,
                       Time time,
                       long requestTimeoutMs,
                       long retryBackoffMs,
                       RebalanceCallback rebalanceCallback) {
        super(new ConsumerGroupController(assignors, subscriptions, metadata, rebalanceCallback),
                client,
                groupId,
                sessionTimeoutMs,
                heartbeatIntervalMs,
                metrics,
                metricGrpPrefix,
                metricTags,
                time,
                requestTimeoutMs,
                retryBackoffMs);
        this.subscriptions = subscriptions;
        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix, metricTags);
    }

    /**
     * Refresh the committed offsets for provided partitions.
     */
    public void refreshCommittedOffsetsIfNeeded() {
        if (subscriptions.refreshCommitsNeeded()) {
            Map<TopicPartition, Long> offsets = fetchCommittedOffsets(subscriptions.assignedPartitions());
            for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                // verify assignment is still active
                if (subscriptions.isAssigned(tp))
                    this.subscriptions.committed(tp, entry.getValue());
            }
            this.subscriptions.commitsRefreshed();
        }
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset
     */
    public Map<TopicPartition, Long> fetchCommittedOffsets(Set<TopicPartition> partitions) {
        while (true) {
            ensureCoordinatorKnown();
            ensurePartitionAssignment();

            // contact coordinator to fetch committed offsets
            RequestFuture<Map<TopicPartition, Long>> future = sendOffsetFetchRequest(partitions);
            client.poll(future);

            if (future.succeeded())
                return future.value();

            if (!future.isRetriable())
                throw future.exception();

            Utils.sleep(retryBackoffMs);
        }
    }

    /**
     * Ensure that we have a valid partition assignment from the coordinator.
     */
    public void ensurePartitionAssignment() {
        if (subscriptions.partitionsAutoAssigned())
            ensureActiveGroup();
    }


    /**
     * Commit offsets. This call blocks (regardless of commitType) until the coordinator
     * can receive the commit request. Once the request has been made, however, only the
     * synchronous commits will wait for a successful response from the coordinator.
     * @param offsets Offsets to commit.
     * @param commitType Commit policy
     * @param callback Callback to be executed when the commit request finishes
     */
    public void commitOffsets(Map<TopicPartition, Long> offsets, CommitType commitType, ConsumerCommitCallback callback) {
        if (commitType == CommitType.ASYNC)
            commitOffsetsAsync(offsets, callback);
        else
            commitOffsetsSync(offsets, callback);
    }

    private void commitOffsetsAsync(final Map<TopicPartition, Long> offsets, final ConsumerCommitCallback callback) {
        this.subscriptions.needRefreshCommits();
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        if (callback != null) {
            future.addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    callback.onComplete(offsets, null);
                }

                @Override
                public void onFailure(RuntimeException e) {
                    callback.onComplete(offsets, e);
                }
            });
        }
    }

    private void commitOffsetsSync(Map<TopicPartition, Long> offsets, ConsumerCommitCallback callback) {
        while (true) {
            ensureCoordinatorKnown();
            ensurePartitionAssignment();

            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future);

            if (future.succeeded()) {
                if (callback != null)
                    callback.onComplete(offsets, null);
                return;
            }

            if (!future.isRetriable()) {
                if (callback == null)
                    throw future.exception();
                else
                    callback.onComplete(offsets, future.exception());
                return;
            }

            Utils.sleep(retryBackoffMs);
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    private RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, Long> offsets) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        // create the offset commit request
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData;
        offsetData = new HashMap<>(offsets.size());
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet())
            offsetData.put(entry.getKey(), new OffsetCommitRequest.PartitionData(entry.getValue(), ""));
        OffsetCommitRequest req = new OffsetCommitRequest(this.groupId,
                this.generation,
                this.memberId,
                OffsetCommitRequest.DEFAULT_RETENTION_TIME,
                offsetData);

        return client.send(coordinator, ApiKeys.OFFSET_COMMIT, req)
                .compose(new OffsetCommitResponseHandler(offsets));
    }


    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {

        private final Map<TopicPartition, Long> offsets;

        public OffsetCommitResponseHandler(Map<TopicPartition, Long> offsets) {
            this.offsets = offsets;
        }

        @Override
        public OffsetCommitResponse parse(ClientResponse response) {
            return new OffsetCommitResponse(response.responseBody());
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitLatency.record(response.requestLatencyMs());
            for (Map.Entry<TopicPartition, Short> entry : commitResponse.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                long offset = this.offsets.get(tp);
                short errorCode = entry.getValue();
                if (errorCode == Errors.NONE.code()) {
                    log.debug("Committed offset {} for partition {}", offset, tp);
                    if (subscriptions.isAssigned(tp))
                        // update the local cache only if the partition is still assigned
                        subscriptions.committed(tp, offset);
                } else if (errorCode == Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code()
                        || errorCode == Errors.NOT_COORDINATOR_FOR_GROUP.code()) {
                    coordinatorDead();
                    future.raise(Errors.forCode(errorCode));
                    return;
                } else if (errorCode == Errors.OFFSET_METADATA_TOO_LARGE.code()
                        || errorCode == Errors.INVALID_COMMIT_OFFSET_SIZE.code()) {
                    // do not need to throw the exception but just log the error
                    log.error("Error committing partition {} at offset {}: {}",
                            tp,
                            offset,
                            Errors.forCode(errorCode).exception().getMessage());
                } else if (errorCode == Errors.UNKNOWN_MEMBER_ID.code()
                        || errorCode == Errors.ILLEGAL_GENERATION.code()) {
                    // need to re-join group
                    subscriptions.needReassignment();
                    future.raise(Errors.forCode(errorCode));
                    return;
                } else {
                    // do not need to throw the exception but just log the error
                    future.raise(Errors.forCode(errorCode));
                    log.error("Error committing partition {} at offset {}: {}",
                            tp,
                            offset,
                            Errors.forCode(errorCode).exception().getMessage());
                }
            }

            future.complete(null);
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, Long>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Fetching committed offsets for partitions: {}",  Utils.join(partitions, ", "));
        // construct the request
        OffsetFetchRequest request = new OffsetFetchRequest(this.groupId, new ArrayList<TopicPartition>(partitions));

        // send the request with a callback
        return client.send(coordinator, ApiKeys.OFFSET_FETCH, request)
                .compose(new OffsetFetchResponseHandler());
    }

    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, Long>> {

        @Override
        public OffsetFetchResponse parse(ClientResponse response) {
            return new OffsetFetchResponse(response.responseBody());
        }

        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, Long>> future) {
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
                        future.raise(Errors.OFFSET_LOAD_IN_PROGRESS);
                    } else if (data.errorCode == Errors.NOT_COORDINATOR_FOR_GROUP.code()) {
                        // re-discover the coordinator and retry
                        coordinatorDead();
                        future.raise(Errors.NOT_COORDINATOR_FOR_GROUP);
                    } else if (data.errorCode == Errors.UNKNOWN_MEMBER_ID.code()
                            || data.errorCode == Errors.ILLEGAL_GENERATION.code()) {
                        // need to re-join group
                        subscriptions.needReassignment();
                        future.raise(Errors.forCode(data.errorCode));
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response: "
                                + Errors.forCode(data.errorCode).exception().getMessage()));
                    }
                    return;
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    offsets.put(tp, data.offset);
                } else {
                    log.debug("No committed offset for partition " + tp);
                }
            }

            future.complete(offsets);
        }
    }

    public interface RebalanceCallback {
        void onPartitionsAssigned(Collection<TopicPartition> partitions);
        void onPartitionsRevoked(Collection<TopicPartition> partitions);
    }

    private class ConsumerCoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor commitLatency;

        public ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> tags) {
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
        }
    }

}
