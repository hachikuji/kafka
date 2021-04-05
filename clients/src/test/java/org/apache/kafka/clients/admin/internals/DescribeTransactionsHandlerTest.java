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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeTransactionsRequest;
import org.apache.kafka.common.requests.DescribeTransactionsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DescribeTransactionsHandlerTest {
    private final LogContext logContext = new LogContext();

    @Test
    public void testBuildRequest() {
        String transactionalId1 = "foo";
        String transactionalId2 = "bar";
        String transactionalId3 = "baz";

        Set<String> transactionalIds = mkSet(transactionalId1, transactionalId2, transactionalId3);
        DescribeTransactionsHandler handler = new DescribeTransactionsHandler(transactionalIds, logContext);

        assertLookup(handler, transactionalIds);
        assertLookup(handler, mkSet(transactionalId1));
        assertLookup(handler, mkSet(transactionalId2, transactionalId3));
    }

    @Test
    public void testHandleSuccessfulResponse() {
        int brokerId = 1;
        String transactionalId1 = "foo";
        String transactionalId2 = "bar";

        Set<String> transactionalIds = mkSet(transactionalId1, transactionalId2);
        DescribeTransactionsHandler handler = new DescribeTransactionsHandler(transactionalIds, logContext);

        DescribeTransactionsResponseData.TransactionState transactionState1 =
            sampleTransactionState1(transactionalId1);
        DescribeTransactionsResponseData.TransactionState transactionState2 =
            sampleTransactionState2(transactionalId2);

        Set<CoordinatorKey> keys = coordinatorKeys(transactionalIds);
        DescribeTransactionsResponse response = new DescribeTransactionsResponse(new DescribeTransactionsResponseData()
            .setTransactionStates(asList(transactionState1, transactionState2)));

        ApiResult<CoordinatorKey, TransactionDescription> result = handler.handleResponse(
            brokerId, keys, response);

        assertEquals(keys, result.completedKeys.keySet());
        assertMatchingTransactionState(brokerId, transactionState1,
            result.completedKeys.get(coordinatorKey(transactionalId1)));
        assertMatchingTransactionState(brokerId, transactionState2,
            result.completedKeys.get(coordinatorKey(transactionalId2)));
    }

    @Test
    public void testHandleErrorResponse() {
        String transactionalId = "foo";
        Set<String> transactionalIds = mkSet(transactionalId);
        DescribeTransactionsHandler handler = new DescribeTransactionsHandler(transactionalIds, logContext);
        assertFatalError(handler, transactionalId, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        assertFatalError(handler, transactionalId, Errors.TRANSACTIONAL_ID_NOT_FOUND);
        assertFatalError(handler, transactionalId, Errors.UNKNOWN_SERVER_ERROR);
        assertRetriableError(handler, transactionalId, Errors.COORDINATOR_LOAD_IN_PROGRESS);
        assertUnmappedKey(handler, transactionalId, Errors.NOT_COORDINATOR);
        assertUnmappedKey(handler, transactionalId, Errors.COORDINATOR_NOT_AVAILABLE);
    }

    private void assertFatalError(
        DescribeTransactionsHandler handler,
        String transactionalId,
        Errors error
    ) {
        CoordinatorKey key = coordinatorKey(transactionalId);
        ApiResult<CoordinatorKey, TransactionDescription> result = handleResponseError(handler, transactionalId, error);
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(mkSet(key), result.failedKeys.keySet());

        Throwable throwable = result.failedKeys.get(key);
        assertTrue(error.exception().getClass().isInstance(throwable));
    }

    private void assertRetriableError(
        DescribeTransactionsHandler handler,
        String transactionalId,
        Errors error
    ) {
        ApiResult<CoordinatorKey, TransactionDescription> result = handleResponseError(handler, transactionalId, error);
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(emptyMap(), result.failedKeys);
    }

    private void assertUnmappedKey(
        DescribeTransactionsHandler handler,
        String transactionalId,
        Errors error
    ) {
        CoordinatorKey key = coordinatorKey(transactionalId);
        ApiResult<CoordinatorKey, TransactionDescription> result = handleResponseError(handler, transactionalId, error);
        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(singletonList(key), result.unmappedKeys);
    }

    private ApiResult<CoordinatorKey, TransactionDescription> handleResponseError(
        DescribeTransactionsHandler handler,
        String transactionalId,
        Errors error
    ) {
        int brokerId = 1;

        CoordinatorKey key = coordinatorKey(transactionalId);
        Set<CoordinatorKey> keys = mkSet(key);

        DescribeTransactionsResponseData.TransactionState transactionState = new DescribeTransactionsResponseData.TransactionState()
            .setErrorCode(error.code())
            .setTransactionalId(transactionalId);

        DescribeTransactionsResponse response = new DescribeTransactionsResponse(new DescribeTransactionsResponseData()
            .setTransactionStates(singletonList(transactionState)));

        ApiResult<CoordinatorKey, TransactionDescription> result = handler.handleResponse(brokerId, keys, response);
        assertEquals(emptyMap(), result.completedKeys);
        return result;
    }

    private void assertLookup(
        DescribeTransactionsHandler handler,
        Set<String> transactionalIds
    ) {
        Set<CoordinatorKey> keys = coordinatorKeys(transactionalIds);
        DescribeTransactionsRequest.Builder request = handler.buildRequest(1, keys);
        assertEquals(transactionalIds, new HashSet<>(request.data.transactionalIds()));
    }

    private static CoordinatorKey coordinatorKey(String transactionalId) {
        return new CoordinatorKey(transactionalId, FindCoordinatorRequest.CoordinatorType.TRANSACTION);
    }

    private static Set<CoordinatorKey> coordinatorKeys(Set<String> transactionalIds) {
        return transactionalIds.stream()
            .map(DescribeTransactionsHandlerTest::coordinatorKey)
            .collect(Collectors.toSet());
    }

    private DescribeTransactionsResponseData.TransactionState sampleTransactionState1(
        String transactionalId
    ) {
        return new DescribeTransactionsResponseData.TransactionState()
            .setErrorCode(Errors.NONE.code())
            .setTransactionState("Ongoing")
            .setTransactionalId(transactionalId)
            .setProducerId(12345L)
            .setProducerEpoch((short) 15)
            .setTransactionStartTimeMs(1599151791L)
            .setTransactionTimeoutMs(10000)
            .setTopics(new DescribeTransactionsResponseData.TopicDataCollection(asList(
                new DescribeTransactionsResponseData.TopicData()
                    .setTopic("foo")
                    .setPartitions(asList(1, 3, 5)),
                new DescribeTransactionsResponseData.TopicData()
                    .setTopic("bar")
                    .setPartitions(asList(1, 3, 5))
            ).iterator()));
    }

    private DescribeTransactionsResponseData.TransactionState sampleTransactionState2(
        String transactionalId
    ) {
        return new DescribeTransactionsResponseData.TransactionState()
            .setErrorCode(Errors.NONE.code())
            .setTransactionState("Empty")
            .setTransactionalId(transactionalId)
            .setProducerId(98765L)
            .setProducerEpoch((short) 30)
            .setTransactionStartTimeMs(-1);
    }

    private void assertMatchingTransactionState(
        int expectedCoordinatorId,
        DescribeTransactionsResponseData.TransactionState expected,
        TransactionDescription actual
    ) {
        assertEquals(expectedCoordinatorId, actual.coordinatorId());
        assertEquals(expected.producerId(), actual.producerId());
        assertEquals(expected.producerEpoch(), actual.producerEpoch());
        assertEquals(expected.transactionTimeoutMs(), actual.transactionTimeoutMs());
        assertEquals(expected.transactionStartTimeMs(), actual.transactionStartTimeMs().orElse(-1));
        assertEquals(collectTransactionPartitions(expected), actual.topicPartitions());
    }

    private Set<TopicPartition> collectTransactionPartitions(
        DescribeTransactionsResponseData.TransactionState transactionState
    ) {
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (DescribeTransactionsResponseData.TopicData topicData : transactionState.topics()) {
            for (Integer partitionId : topicData.partitions()) {
                topicPartitions.add(new TopicPartition(topicData.topic(), partitionId));
            }
        }
        return topicPartitions;
    }

}
