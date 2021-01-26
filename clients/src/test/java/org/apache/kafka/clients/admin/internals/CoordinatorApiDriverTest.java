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

import org.apache.kafka.clients.admin.internals.ApiDriver.RequestSpec;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CoordinatorApiDriverTest {
    private final MockTime time = new MockTime();
    private final long deadlineMs = time.milliseconds() + 10000;
    private final long retryBackoffMs = 100;

    @Test
    public void testLookupGrouping() {
        CoordinatorKey group1 = new CoordinatorKey("foo", CoordinatorType.GROUP);
        CoordinatorKey group2 = new CoordinatorKey("bar", CoordinatorType.GROUP);
        Set<CoordinatorKey> groupIds = mkSet(group1, group2);

        TestCoordinatorDriver driver = new TestCoordinatorDriver(groupIds);
        List<RequestSpec<CoordinatorKey>> requests = driver.poll();
        assertEquals(2, requests.size());

        // While the FindCoordinator requests are inflight, we will not send any more
        assertEquals(0, driver.poll().size());

        RequestSpec<CoordinatorKey> spec1 = lookupRequest(requests, group1);
        assertEquals(mkSet(group1), spec1.keys);
        assertEquals(OptionalInt.empty(), spec1.scope.destinationBrokerId());
        assertEquals(deadlineMs, spec1.deadlineMs);
        assertEquals(0, spec1.tries);
        assertEquals(0, spec1.nextAllowedTryMs);
        assertTrue(spec1.request instanceof FindCoordinatorRequest.Builder);
        FindCoordinatorRequest.Builder findCoordinatorRequest1 = (FindCoordinatorRequest.Builder) spec1.request;
        assertEquals(group1.idValue, findCoordinatorRequest1.data().key());
        assertEquals(group1.type.id(), findCoordinatorRequest1.data().keyType());

        RequestSpec<CoordinatorKey> spec2 = lookupRequest(requests, group2);
        assertEquals(mkSet(group2), spec2.keys);
        assertEquals(OptionalInt.empty(), spec2.scope.destinationBrokerId());
        assertEquals(deadlineMs, spec2.deadlineMs);
        assertEquals(0, spec2.tries);
        assertEquals(0, spec2.nextAllowedTryMs);
        assertTrue(spec2.request instanceof FindCoordinatorRequest.Builder);
        FindCoordinatorRequest.Builder findCoordinatorRequest2 = (FindCoordinatorRequest.Builder) spec2.request;
        assertEquals(group2.idValue, findCoordinatorRequest2.data().key());
        assertEquals(group2.type.id(), findCoordinatorRequest2.data().keyType());
    }

    @Test
    public void testSuccessfulLeaderDiscovery() {
        CoordinatorKey group1 = new CoordinatorKey("foo", CoordinatorType.GROUP);
        CoordinatorKey group2 = new CoordinatorKey("bar", CoordinatorType.GROUP);
        Set<CoordinatorKey> groupIds = mkSet(group1, group2);

        TestCoordinatorDriver driver = new TestCoordinatorDriver(groupIds);
        List<RequestSpec<CoordinatorKey>> lookupRequests = driver.poll();
        assertEquals(2, lookupRequests.size());

        RequestSpec<CoordinatorKey> lookupSpec1 = lookupRequest(lookupRequests, group1);
        driver.onResponse(time.milliseconds(), lookupSpec1, new FindCoordinatorResponse(new FindCoordinatorResponseData()
            .setErrorCode(Errors.NONE.code())
            .setHost("localhost")
            .setPort(9092)
            .setNodeId(1)
        ));

        List<RequestSpec<CoordinatorKey>> requests1 = driver.poll();
        assertEquals(1, requests1.size());
        RequestSpec<CoordinatorKey> requestSpec1 = requests1.get(0);
        assertEquals(mkSet(group1), requestSpec1.keys);
        assertEquals(OptionalInt.of(1), requestSpec1.scope.destinationBrokerId());
        assertEquals(0, requestSpec1.tries);
        assertEquals(deadlineMs, requestSpec1.deadlineMs);
        assertEquals(0, requestSpec1.nextAllowedTryMs);
        assertTrue(requestSpec1.request instanceof DescribeGroupsRequest.Builder);
        DescribeGroupsRequest.Builder request = (DescribeGroupsRequest.Builder) requestSpec1.request;
        assertEquals(singletonList(group1.idValue), request.data.groups());

        RequestSpec<CoordinatorKey> lookupSpec2 = lookupRequest(lookupRequests, group2);
        driver.onResponse(time.milliseconds(), lookupSpec2, new FindCoordinatorResponse(new FindCoordinatorResponseData()
            .setErrorCode(Errors.NONE.code())
            .setHost("localhost")
            .setPort(9093)
            .setNodeId(2)
        ));

        List<RequestSpec<CoordinatorKey>> requests2 = driver.poll();
        assertEquals(1, requests2.size());
        RequestSpec<CoordinatorKey> requestSpec2 = requests2.get(0);
        assertEquals(mkSet(group2), requestSpec2.keys);
        assertEquals(OptionalInt.of(2), requestSpec2.scope.destinationBrokerId());
        assertEquals(0, requestSpec2.tries);
        assertEquals(deadlineMs, requestSpec2.deadlineMs);
        assertEquals(0, requestSpec2.nextAllowedTryMs);
        assertTrue(requestSpec2.request instanceof DescribeGroupsRequest.Builder);
        DescribeGroupsRequest.Builder request2 = (DescribeGroupsRequest.Builder) requestSpec2.request;
        assertEquals(singletonList(group2.idValue), request2.data.groups());
    }

    @Test
    public void testRetriableFindCoordinatorError() {
        CoordinatorKey group1 = new CoordinatorKey("foo", CoordinatorType.GROUP);
        Set<CoordinatorKey> groupIds = mkSet(group1);

        TestCoordinatorDriver driver = new TestCoordinatorDriver(groupIds);
        List<RequestSpec<CoordinatorKey>> lookupRequests1 = driver.poll();
        assertEquals(1, lookupRequests1.size());

        RequestSpec<CoordinatorKey> lookupSpec = lookupRequests1.get(0);
        driver.onResponse(time.milliseconds(), lookupSpec, new FindCoordinatorResponse(new FindCoordinatorResponseData()
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
        ));

        List<RequestSpec<CoordinatorKey>> lookupRequests2 = driver.poll();
        assertEquals(1, lookupRequests1.size());

        RequestSpec<CoordinatorKey> retryLookupSpec = lookupRequests2.get(0);
        assertEquals(1, retryLookupSpec.tries);
        assertEquals(time.milliseconds() + retryBackoffMs, retryLookupSpec.nextAllowedTryMs);
        assertEquals(deadlineMs, retryLookupSpec.deadlineMs);
        assertEquals(groupIds, retryLookupSpec.keys);
        assertEquals(OptionalInt.empty(), retryLookupSpec.scope.destinationBrokerId());
    }

    @Test
    public void testFatalFindCoordinatorError() {
        CoordinatorKey group1 = new CoordinatorKey("foo", CoordinatorType.GROUP);
        Set<CoordinatorKey> groupIds = mkSet(group1);

        TestCoordinatorDriver driver = new TestCoordinatorDriver(groupIds);
        List<RequestSpec<CoordinatorKey>> lookupRequests1 = driver.poll();
        assertEquals(1, lookupRequests1.size());

        RequestSpec<CoordinatorKey> lookupSpec = lookupRequests1.get(0);
        driver.onResponse(time.milliseconds(), lookupSpec, new FindCoordinatorResponse(new FindCoordinatorResponseData()
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code())
        ));

        assertEquals(Collections.emptyList(), driver.poll());
        GroupAuthorizationException groupAuthorizationException = assertFutureThrows(
            driver.futures().get(group1), GroupAuthorizationException.class);
        assertEquals(group1.idValue, groupAuthorizationException.groupId());
    }

    private RequestSpec<CoordinatorKey> lookupRequest(
        List<RequestSpec<CoordinatorKey>> requests,
        CoordinatorKey key
    ) {
        Optional<RequestSpec<CoordinatorKey>> foundRequestOpt = requests.stream()
            .filter(spec -> spec.keys.contains(key))
            .findFirst();
        assertTrue(foundRequestOpt.isPresent());
        return foundRequestOpt.get();
    }

    private final class TestCoordinatorDriver extends CoordinatorApiDriver<String> {

        public TestCoordinatorDriver(Set<CoordinatorKey> groupIds) {
            super(groupIds, deadlineMs, retryBackoffMs, new LogContext());
        }

        @Override
        String apiName() {
            return "testCoordinatorApi";
        }

        @Override
        AbstractRequest.Builder<?> buildFulfillmentRequest(Integer brokerId, Set<CoordinatorKey> coordinatorKeys) {
            return new DescribeGroupsRequest.Builder(new DescribeGroupsRequestData()
                .setGroups(coordinatorKeys.stream()
                    .map(coordinatorKey -> coordinatorKey.idValue)
                    .collect(Collectors.toList())));
        }

        @Override
        void handleFulfillmentResponse(Integer brokerId, Set<CoordinatorKey> keys, AbstractResponse response) {
            throw new UnsupportedOperationException();
        }
    }

}
