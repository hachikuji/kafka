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

package kafka.test.junit;

import kafka.network.SocketServer;
import kafka.server.BrokerServer;
import kafka.server.ControllerServer;
import kafka.server.KafkaConfig;
import kafka.test.ClusterConfig;
import kafka.test.ClusterInstance;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.metadata.BrokerState;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Wraps a {@link KafkaClusterTestKit} inside lifecycle methods for a test invocation. Each instance of this
 * class is provided with a configuration for the cluster.
 *
 * This context also provides parameter resolvers for:
 *
 * <ul>
 *     <li>ClusterConfig (the same instance passed to the constructor)</li>
 *     <li>ClusterInstance (includes methods to expose underlying SocketServer-s)</li>
 *     <li>IntegrationTestHelper (helper methods)</li>
 * </ul>
 */
public class RaftClusterInvocationContext implements TestTemplateInvocationContext {

    private final ClusterConfig clusterConfig;
    private final AtomicReference<KafkaClusterTestKit> clusterReference;

    public RaftClusterInvocationContext(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.clusterReference = new AtomicReference<>();
    }

    @Override
    public String getDisplayName(int invocationIndex) {
        String clusterDesc = clusterConfig.nameTags().entrySet().stream()
            .map(Object::toString)
            .collect(Collectors.joining(", "));
        return String.format("[Quorum %d] %s", invocationIndex, clusterDesc);
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        return Arrays.asList(
            (BeforeTestExecutionCallback) context -> {
                TestKitNodes testKitNodes = new TestKitNodes.Builder().
                    setNumKip500BrokerNodes(clusterConfig.numBrokers()).
                    setNumControllerNodes(clusterConfig.numControllers()).build();

                Function<ClusterConfig.ProcessSpec, Properties> configGenerator = spec -> {
                    Properties properties = new Properties();
                    properties.putAll(clusterConfig.serverProperties());
                    properties.putAll(clusterConfig.serverOverrides().apply(spec));
                    return properties;
                };

                KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(
                    testKitNodes, configGenerator);

                // KAFKA-12512 need to pass security protocol and listener name here
                KafkaClusterTestKit cluster = builder.build();
                clusterReference.set(cluster);
                cluster.format();
                cluster.startup();
                kafka.utils.TestUtils.waitUntilTrue(
                    () -> cluster.brokers().get(0).currentState() == BrokerState.RUNNING,
                    () -> "Broker never made it to RUNNING state.",
                    org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS,
                    100L);

                cluster.waitForReadyBrokers();

                if (clusterConfig.shouldAutoCreateOffsetsTopic()) {
                    BrokerServer server = cluster.brokers().values().stream().findFirst().orElseThrow(() ->
                        new IllegalStateException("No brokers have been initialized"));

                    KafkaConfig config = server.config();
                    Admin admin = Admin.create(cluster.clientProperties());

                    CreateTopicsResult result = admin.createTopics(Collections.singleton(new NewTopic(
                        Topic.GROUP_METADATA_TOPIC_NAME,
                        config.offsetsTopicPartitions(),
                        config.offsetsTopicReplicationFactor()
                    )));

                    // TODO: Initialize all the topic configs as well
                    result.all().get();
                }
            },
            (AfterTestExecutionCallback) context -> clusterReference.get().close(),
            new ClusterInstanceParameterResolver(new RaftClusterInstance(clusterReference, clusterConfig)),
            new GenericParameterResolver<>(clusterConfig, ClusterConfig.class)
        );
    }

    public static class RaftClusterInstance implements ClusterInstance {
        private static final Logger log = LoggerFactory.getLogger(RaftClusterInstance.class);

        private final AtomicReference<KafkaClusterTestKit> clusterReference;
        private final ClusterConfig clusterConfig;
        final AtomicBoolean started = new AtomicBoolean(false);
        final AtomicBoolean stopped = new AtomicBoolean(false);
        private final ConcurrentLinkedQueue<Admin> admins = new ConcurrentLinkedQueue<>();

        RaftClusterInstance(AtomicReference<KafkaClusterTestKit> clusterReference, ClusterConfig clusterConfig) {
            this.clusterReference = clusterReference;
            this.clusterConfig = clusterConfig;
        }

        @Override
        public String bootstrapServers() {
            return clusterReference.get().clientProperties().getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        }

        @Override
        public Collection<SocketServer> brokerSocketServers() {
            return clusterReference.get().brokers().values().stream()
                .map(BrokerServer::socketServer)
                .collect(Collectors.toList());
        }

        @Override
        public SocketServer brokerSocketServer(Integer brokerId) {
            return brokerSocketServers().stream()
                .filter(server -> server.config().nodeId() == brokerId)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Could not find socket server for broker " + brokerId));
        }

        @Override
        public ListenerName clientListener() {
            return ListenerName.normalised("EXTERNAL");
        }

        @Override
        public Collection<SocketServer> controllerSocketServers() {
            return clusterReference.get().controllers().values().stream()
                .map(ControllerServer::socketServer)
                .collect(Collectors.toList());
        }

        @Override
        public SocketServer anyBrokerSocketServer() {
            return clusterReference.get().brokers().values().stream()
                .map(BrokerServer::socketServer)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No broker SocketServers found"));
        }

        @Override
        public SocketServer anyControllerSocketServer() {
            return clusterReference.get().controllers().values().stream()
                .map(ControllerServer::socketServer)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No controller SocketServers found"));
        }

        @Override
        public ClusterType clusterType() {
            return ClusterType.RAFT;
        }

        @Override
        public ClusterConfig config() {
            return clusterConfig;
        }

        @Override
        public KafkaClusterTestKit getUnderlying() {
            return clusterReference.get();
        }

        @Override
        public Admin createAdminClient(Properties configOverrides) {
            Admin admin = Admin.create(clusterReference.get().clientProperties());
            admins.add(admin);
            return admin;
        }

        @Override
        public void start() {
            if (started.compareAndSet(false, true)) {
                try {
                    clusterReference.get().startup();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to start Raft server", e);
                }
            }
        }

        @Override
        public void stop() {
            if (stopped.compareAndSet(false, true)) {
                try {
                    clusterReference.get().close();
                } catch (Exception e) {
                    log.error("Failed to stop Raft server", e);
                }

                for (Admin admin : admins) {
                    try {
                        admin.close(Duration.ZERO);
                    } catch (Exception e) {
                        log.error("Failed to stop admin client", e);
                    }
                }
            }
        }
    }
}