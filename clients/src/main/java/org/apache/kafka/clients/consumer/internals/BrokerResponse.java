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

import org.apache.kafka.common.KafkaException;

public class BrokerResponse<T> implements DelayedResponse<T> {

    private T value = null;
    protected boolean ready = false;
    private boolean needMetadataRefresh = false;
    private boolean needRetry = false;

    private boolean needThrow = false;
    private KafkaException exception;

    @Override
    public T value() {
        return value;
    }

    @Override
    public boolean isReady() {
        return ready;
    }

    @Override
    public boolean hasValue() {
        return value != null;
    }

    public void respond(T result) {
        this.value = result;
        this.ready = true;
    }

    public boolean metadataRefreshNeeded() {
        return needMetadataRefresh;
    }

    public boolean retryNeeded() {
        return needRetry;
    }

    public void needMetadataRefresh() {
        this.needMetadataRefresh = true;
        this.ready = true;
    }

    public void needRetry() {
        this.needRetry = true;
        this.ready = true;
    }

    public void raise(KafkaException e) {
        this.needThrow = true;
        this.exception = e;
    }

    public boolean throwExceptionNeeded() {
        return needThrow;
    }

    public KafkaException exception() {
        return exception;
    }

}
