/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.tools;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MockSinkConnector extends SinkConnector {

    public static final String MOCK_MODE_KEY = "mock-mode";
    public static final String DELAY_MS_KEY = "delay";

    public static final String CONNECTOR_FAILURE = "connector-failure";
    public static final String TASK_FAILURE = "task-failure";

    public static final long DEFAULT_FAILURE_DELAY_MS = 15000;

    private Map<String, String> config;
    private ScheduledExecutorService executor;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> config) {
        this.config = config;

        if (CONNECTOR_FAILURE.equals(config.get(MOCK_MODE_KEY))) {
            String delayMsString = config.get(DELAY_MS_KEY);
            long delayMs = DEFAULT_FAILURE_DELAY_MS;
            if (delayMsString != null)
                delayMs = Long.parseLong(delayMsString);

            executor = Executors.newSingleThreadScheduledExecutor();
            executor.schedule(new Runnable() {
                @Override
                public void run() {
                    context.raiseError(new RuntimeException());
                }
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MockSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.singletonList(config);
    }

    @Override
    public void stop() {
        if (executor != null)
            executor.shutdownNow();
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

}
