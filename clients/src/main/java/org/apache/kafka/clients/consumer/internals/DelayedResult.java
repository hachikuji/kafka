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
 * Result of an asynchronous request.  It is similar to a Future, but it does not provide the mechanics to
 * handle itself. Instead, you must use {@link org.apache.kafka.clients.KafkaClient#poll(long, long)} until
 * a response is ready. Blocking usage would typically look something like this:
 *
 * <pre>
 *     DelayedResult response = initializeRequest();
 *     do {
 *         client.poll(timeout, now);
 *     } while (!response.isReady());
 * </pre>
 *
 * When {@link #isReady()} returns true, then one of {@link #failed()} or {@link #succeeded()}
 * will return true. If the result succeeded and there is a value corresponding with the result, then it
 * can be obtained with {@link #value()}.
 *
 * If the result failed, then there are two possibilities: either there is an exception to handle, or
 * there is a remedy to apply. Remedies are used to signal the caller that the request should be retried
 * or that some other action should be taken. Exceptions indicate an error that generally cannot be handled
 * and must be propagated.
 *
 * @param <T> Return type of the result (Can be Void if there is no response)
 */
public class DelayedResult<T, R> {
    private boolean ready = false;
    private boolean succeeded = false;
    private R remedy;
    private T value;
    private RuntimeException exception;

    /**
     * Check whether the response is ready to be handled
     * @return true if the response is ready, false otherwise
     */
    public boolean isReady() {
        return ready;
    }

    /**
     * Get the value corresponding to this request (if it has one, as indicated by {@link #succeeded()}).
     * @return the value if it exists or null
     */
    public T value() {
        return value;
    }

    /**
     * Check if a value was returned from the response. Use after {@link #isReady()} returns true.
     * @return true if a value is available, false otherwise
     */
    public boolean succeeded() {
        return ready && succeeded;
    }

    /**
     * Return the error from this response (assuming {@link #failed()} has returned true. If the
     * response is not ready or if there is no error, null is returned.
     * @return the error if it exists or null
     */
    public R remedy() {
        return remedy;
    }

    /**
     * Check if there is a remedy for a failed response.
     * @return true if the result is ready and there is a remedy
     */
    public boolean hasRemedy() {
        return ready && remedy != null;
    }

    /**
     * Check if the result failed (which is true both when {@link #fail(Object)} and {@link #raise(RuntimeException)}
     * are used. Use after {@link #isReady()} returns true.
     * @return true if a failure occurred, false otherwise
     */
    public boolean failed() {
        return ready && !succeeded;
    }

    /**
     * Get the exception from a failed result. You should check that there is an exception
     * with {@link #hasException()} before using this method.
     * @return The exception if it exists or null
     */
    public RuntimeException exception() {
        return exception;
    }

    /**
     * Check whether there is an exception. This is only possible if the result is ready and
     * {@link #failed()} returns true.
     * @return true if the result is ready and there is an exception, false otherwise
     */
    public boolean hasException() {
        return ready && exception != null;
    }

    /**
     * Fail the result with a possible remediation. This allows the caller to take some action in
     * response to the failure (such as retrying the request).
     * @param remedy The action that can be taken by the caller to recover from the failure
     */
    public void fail(R remedy) {
        this.ready = true;
        this.succeeded = false;
        this.remedy = remedy;
    }

    /**
     * Complete the request successfully. After this call, {@link #succeeded()} will return true
     * and the value can be obtained through {@link #value()}.
     * @param value corresponding value (or null if there is none)
     */
    public void complete(T value) {
        this.ready = true;
        this.succeeded = true;
        this.value = value;
    }

    /**
     * Raise an exception. The result will be marked as failed, and the caller can either
     * handle the exception or throw it.
     * @param e The exception that
     */
    public void raise(RuntimeException e){
        this.ready = true;
        this.succeeded = false;
        this.exception = e;
    }


}
