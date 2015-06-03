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

/**
 * Delayed response subclass for responses from the consumer group coordinator.
 * @param <T>
 */
public class CoordinatorResult<T> extends DelayedResult<T, CoordinatorResult.CoordinatorRemedy> {
    public static final CoordinatorResult<Object> NEED_NEW_COORDINATOR = new CoordinatorResult<Object>();
    public static final CoordinatorResult<Object> NEED_RETRY = new CoordinatorResult<Object>();

    static {
        NEED_NEW_COORDINATOR.fail(CoordinatorRemedy.FIND_COORDINATOR);
        NEED_RETRY.fail(CoordinatorRemedy.RETRY);
    }

    public enum CoordinatorRemedy {
        RETRY, FIND_COORDINATOR
    }

    public void needRetry() {
        fail(CoordinatorRemedy.RETRY);
    }

    public void needNewCoordinator() {
        fail(CoordinatorRemedy.FIND_COORDINATOR);
    }

    @SuppressWarnings("unchecked")
    public static <T> CoordinatorResult<T> retryNeeded() {
        return (CoordinatorResult<T>) NEED_RETRY;
    }

    @SuppressWarnings("unchecked")
    public static <T> CoordinatorResult<T> newCoordinatorNeeded() {
        return (CoordinatorResult<T>) NEED_NEW_COORDINATOR;
    }

}
