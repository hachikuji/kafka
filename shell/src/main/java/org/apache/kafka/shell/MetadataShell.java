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

package org.apache.kafka.shell;

import kafka.raft.KafkaRaftManager;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.Server;
import kafka.server.MetaProperties;
import kafka.tools.TerseFailure;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.RaftConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.compat.java8.OptionConverters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * The Kafka metadata tool.
 */
public final class MetadataShell {
    private static final Logger log = LoggerFactory.getLogger(MetadataShell.class);

    public static class Builder {
        private String controllers;
        private String configPath;
        private File tempDir;
        private String snapshotPath;

        public Builder setControllers(String controllers) {
            this.controllers = controllers;
            return this;
        }

        public Builder setConfigPath(String configPath) {
            this.configPath = configPath;
            return this;
        }

        public Builder setSnapshotPath(String snapshotPath) {
            this.snapshotPath = snapshotPath;
            return this;
        }

        public Builder setTempDir(File tempDir) {
            this.tempDir = tempDir;
            return this;
        }

        public MetadataShell build() throws Exception {
            if (snapshotPath != null) {
                if (controllers != null) {
                    throw new RuntimeException("If you specify a snapshot path, you " +
                        "must not also specify controllers to connect to.");
                }
                return buildWithSnapshotReader();
            } else {
                return buildWithControllerConnect();
            }
        }

        public MetadataShell buildWithControllerConnect() throws Exception {
            Properties properties = null;
            if (configPath != null) {
                properties = Utils.loadProps(configPath);
            } else {
                properties = new Properties();
            }
            if (controllers != null) {
                properties.setProperty(RaftConfig.QUORUM_VOTERS_CONFIG,
                    controllers);
            }
            if (properties.getProperty(RaftConfig.QUORUM_VOTERS_CONFIG) == null) {
                throw new TerseFailure("Please use --controllers to specify the quorum voters.");
            }
            // TODO: we really shouldn't have to set up a fake broker config like this.
            // In particular, it should be possible to run the KafkRaftManager without
            // using a log directory at all.  And we should be able to set -1 as our ID,
            // since we're not a voter.
            final int fakeId = 100; //Integer.MAX_VALUE;
            properties.setProperty(KafkaConfig$.MODULE$.MetadataLogDirProp(),
                tempDir.getAbsolutePath());
            properties.remove(KafkaConfig$.MODULE$.LogDirProp());
            properties.remove(KafkaConfig$.MODULE$.LogDirsProp());
            properties.remove(KafkaConfig$.MODULE$.ControllerIdProp());
            properties.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), Integer.toString(fakeId));
            properties.setProperty(KafkaConfig$.MODULE$.ProcessRolesProp(), "broker");
            KafkaConfig config = new KafkaConfig(properties);
            MetaProperties metaProperties = MetaProperties.apply(Uuid.ZERO_UUID,
                OptionConverters.toScala(Optional.of(fakeId)),
                OptionConverters.toScala(Optional.empty()));
            TopicPartition metadataPartition =
                new TopicPartition(Server.metadataTopicName(), 0);
            KafkaRaftManager raftManager = null;
            MetadataNodeManager nodeManager = null;
            try {
                raftManager = new KafkaRaftManager(metaProperties,
                    metadataPartition,
                    config,
                    Time.SYSTEM,
                    new Metrics(),
                    CompletableFuture.completedFuture(config.quorumVoters()));
                nodeManager = new MetadataNodeManager();
            } catch (Throwable e) {
                log.error("Initialization error", e);
                if (raftManager != null) {
                    raftManager.shutdown();
                }
                if (nodeManager != null) {
                    nodeManager.close();
                }
                throw e;
            }
            return new MetadataShell(raftManager, null, nodeManager);
        }

        public MetadataShell buildWithSnapshotReader() throws Exception {
            MetadataNodeManager nodeManager = null;
            SnapshotReader snapshotReader = null;
            try {
                nodeManager = new MetadataNodeManager();
                snapshotReader = new SnapshotReader(snapshotPath, nodeManager.logListener());
                return new MetadataShell(null, snapshotReader, nodeManager);
            } catch (Throwable e) {
                log.error("Initialization error", e);
                if (snapshotReader != null) {
                    snapshotReader.close();
                }
                if (nodeManager != null) {
                    nodeManager.close();
                }
                throw e;
            }
        }
    }

    private final KafkaRaftManager raftManager;

    private final SnapshotReader snapshotReader;

    private final MetadataNodeManager nodeManager;

    public MetadataShell(KafkaRaftManager raftManager,
                        SnapshotReader snapshotReader,
                        MetadataNodeManager nodeManager) {
        this.raftManager = raftManager;
        this.snapshotReader = snapshotReader;
        this.nodeManager = nodeManager;
    }

    public void run(List<String> args) throws Exception {
        nodeManager.setup();
        if (raftManager != null) {
            raftManager.startup();
            raftManager.register(nodeManager.logListener());
        } else if (snapshotReader != null) {
            snapshotReader.startup();
        } else {
            throw new RuntimeException("Expected either a raft manager or snapshot reader");
        }
        if (args == null || args.isEmpty()) {
            // Interactive mode.
            try (InteractiveShell shell = new InteractiveShell(nodeManager)) {
                shell.runMainLoop();
            }
        } else {
            // Non-interactive mode.
            Commands commands = new Commands(false);
            try (PrintWriter writer = new PrintWriter(new BufferedWriter(
                    new OutputStreamWriter(System.out, StandardCharsets.UTF_8)))) {
                Commands.Handler handler = commands.parseCommand(args);
                handler.run(Optional.empty(), writer, nodeManager);
                writer.flush();
            }
        }
    }

    public void close() throws Exception {
        if (raftManager != null) {
            raftManager.shutdown();
        }
        if (snapshotReader != null) {
            snapshotReader.close();
        }
        nodeManager.close();
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("metadata-tool")
            .defaultHelp(true)
            .description("The Apache Kafka metadata tool");
        parser.addArgument("--controllers", "-C")
            .type(String.class)
            .help("The quorum voter connection string to use.");
        parser.addArgument("--config", "-c")
            .type(String.class)
            .help("The configuration file to use.");
        parser.addArgument("--snapshot", "-s")
            .type(String.class)
            .help("The snapshot file to read.");
        parser.addArgument("command")
            .nargs("*")
            .help("The command to run.");
        Namespace res = parser.parseArgsOrFail(args);
        try {
            Builder builder = new Builder();
            builder.setControllers(res.getString("controllers"));
            builder.setConfigPath(res.getString("config"));
            builder.setSnapshotPath(res.getString("snapshot"));
            Path tempDir = Files.createTempDirectory("MetadataShell");
            Exit.addShutdownHook("agent-shutdown-hook", () -> {
                log.debug("Removing temporary directory " + tempDir.toAbsolutePath().toString());
                try {
                    Utils.delete(tempDir.toFile());
                } catch (Exception e) {
                    log.error("Got exception while removing temporary directory " +
                        tempDir.toAbsolutePath().toString());
                }
            });
            builder.setTempDir(tempDir.toFile());
            MetadataShell shell = builder.build();
            try {
                shell.run(res.getList("command"));
            } finally {
                shell.close();
            }
            Exit.exit(0);
        } catch (TerseFailure e) {
            System.err.println("Error: " + e.getMessage());
            Exit.exit(1);
        } catch (Throwable e) {
            System.err.println("Unexpected error: " +
                (e.getMessage() == null ? "" : e.getMessage()));
            e.printStackTrace(System.err);
            Exit.exit(1);
        }
    }
}
