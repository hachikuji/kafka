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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.InterBrokerApiVersion;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InterBrokerRequestTest {

    private final Class<? extends AbstractRequest> requestClass;
    private final ApiKeys api;
    private final InterBrokerApiVersion minSupportedKafkaVersion;

    public InterBrokerRequestTest(Class<? extends AbstractRequest> requestClass,
                                  ApiKeys api,
                                  InterBrokerApiVersion minSupportedKafkaVersion) {
        this.requestClass = requestClass;
        this.api = api;
        this.minSupportedKafkaVersion = minSupportedKafkaVersion;
    }

    private short mapInterBrokerProtocolVersion(InterBrokerApiVersion version) {
        try {
            Method mapInterBrokerProtocolVersion =
                    requestClass.getDeclaredMethod("mapInterBrokerProtocolVersion", InterBrokerApiVersion.class);
            return (short) mapInterBrokerProtocolVersion.invoke(null, version);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException)
                throw (RuntimeException) e.getCause();
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testInterBrokerProtocolMapping() {
        assertEquals(api.latestVersion(), mapInterBrokerProtocolVersion(InterBrokerApiVersion.latestVersion()));

        for (InterBrokerApiVersion apiVersion : InterBrokerApiVersion.values()) {
            if (apiVersion.compareTo(minSupportedKafkaVersion) < 0) {
                TestUtils.assertThrows(IllegalArgumentException.class, () -> mapInterBrokerProtocolVersion(apiVersion));
            } else {
                short version = mapInterBrokerProtocolVersion(apiVersion);
                assertTrue(api.isVersionSupported(version));
            }
        }
    }

}
