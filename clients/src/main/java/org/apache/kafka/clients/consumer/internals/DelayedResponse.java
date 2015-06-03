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
 * Result of an asynchronous request. Can be waited by calling poll.
 * @param <T>
 */
public interface DelayedResponse<T> {

    /**
     * Get the value corresponding to this request (if it has one, as indicated by {@link #hasValue()}).
     * @return the value or null if none exists
     */
    T value();

    /**
     * Check whether the response is ready to be handled
     * @return true if the response is ready, false otherwise
     */
    boolean isReady();

    /**
     * Delayed responses may have a value. This checks whether one is available. It only makes sense
     * to check this if the response is ready (as indicated by {@link #isReady()}.
     * @return true if a value is available, false otherwise
     */
    boolean hasValue();

}
