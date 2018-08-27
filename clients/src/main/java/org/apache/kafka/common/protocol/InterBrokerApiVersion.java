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
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.record.RecordVersion;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the different Kafka versions.
 * Right now, we use them for upgrades - users can configure the version of the API brokers will use to communicate between themselves.
 * This is only for inter-broker communications - when communicating with clients, the client decides on the API version.
 *
 * Note that the ID we initialize for each version is important.
 * We consider a version newer than another, if it has a higher ID (to avoid depending on lexicographic order)
 *
 * Since the api protocol may change more than once within the same release and to facilitate people deploying code from
 * trunk, we have the concept of internal versions (first introduced during the 0.10.0 development cycle). For example,
 * the first time we introduce a version change in a release, say 0.10.0, we will add a config value "0.10.0-IV0" and a
 * corresponding case object KAFKA_0_10_0-IV0. We will also add a config value "0.10.0" that will be mapped to the
 * latest internal version object, which is KAFKA_0_10_0-IV0. When we change the protocol a second time while developing
 * 0.10.0, we will add a new config value "0.10.0-IV1" and a corresponding case object KAFKA_0_10_0-IV1. We will change
 * the config value "0.10.0" to map to the latest internal version object KAFKA_0_10_0-IV1. The config value of
 * "0.10.0-IV0" is still mapped to KAFKA_0_10_0-IV0. This way, if people are deploying from trunk, they can use
 * "0.10.0-IV0" and "0.10.0-IV1" to upgrade one internal version at a time. For most people who just want to use
 * released version, they can use "0.10.0" when upgrading to the 0.10.0 release.
 */
public enum InterBrokerApiVersion {
    KAFKA_0_8_0(0, "0.8.0", RecordVersion.V0),
    KAFKA_0_8_1(1, "0.8.1", RecordVersion.V0),
    KAFKA_0_8_2(2, "0.8.2", RecordVersion.V0),
    KAFKA_0_9_0(3, "0.9.0", RecordVersion.V0),

    // 0.10.0-IV0 is introduced for KIP-31/32 which changes the message format.
    KAFKA_0_10_0_IV0(4, "0.10.0", "IV0", RecordVersion.V1),
    // 0.10.0-IV1 is introduced for KIP-36(rack awareness) and KIP-43(SASL handshake).
    KAFKA_0_10_0_IV1(5, "0.10.0", "IV1", RecordVersion.V1),
    // introduced for JoinGroup protocol change in KIP-62
    KAFKA_0_10_1_IV0(6, "0.10.1", "IV0", RecordVersion.V1),
    // 0.10.1-IV1 is introduced for KIP-74(fetch response size limit).
    KAFKA_0_10_1_IV1(7, "0.10.1", "IV1", RecordVersion.V1),
    // introduced ListOffsetRequest v1 in KIP-79
    KAFKA_0_10_1_IV2(8, "0.10.1", "IV2", RecordVersion.V1),
    // introduced UpdateMetadataRequest v3 in KIP-103
    KAFKA_0_10_2_IV0(9, "0.10.2", "IV0", RecordVersion.V1),
    // KIP-98 (idempotent and transactional producer support)
    KAFKA_0_11_0_IV0(10, "0.11.0", "IV0", RecordVersion.V2),
    // introduced DeleteRecordsRequest v0 and FetchRequest v4 in KIP-107
    KAFKA_0_11_0_IV1(11, "0.11.0", "IV1", RecordVersion.V2),
    // Introduced leader epoch fetches to the replica fetcher via KIP-101
    KAFKA_0_11_0_IV2(12, "0.11.0", "IV2", RecordVersion.V2),
    // Introduced LeaderAndIsrRequest V1, UpdateMetadataRequest V4 and FetchRequest V6 via KIP-112
    KAFKA_1_0_IV0(13, "1.0", "IV0", RecordVersion.V2),
    // Introduced DeleteGroupsRequest V0 via KIP-229, plus KIP-227 incremental fetch requests,
    // and KafkaStorageException for fetch requests.
    KAFKA_1_1_IV0(14, "1.1", "IV0", RecordVersion.V2),
    // Introduced OffsetsForLeaderEpochRequest V1 via KIP-279 (Fix log divergence between leader and follower after fast leader fail over)
    KAFKA_2_0_IV0(15, "2.0", "IV0", RecordVersion.V2),
    // Several request versions were bumped due to KIP-219 (Improve quota communication)
    KAFKA_2_0_IV1(16, "2.0", "IV1", RecordVersion.V2),
    // Introduced new schemas for group offset (v2) and group metadata (v2) (KIP-211)
    KAFKA_2_1_IV0(17, "2.1", "IV0", RecordVersion.V2);

    public final int id;
    public final String shortVersion;
    private final String subVersion;
    public final RecordVersion recordVersion;

    private static final Map<String, InterBrokerApiVersion> VERSION_MAP = new HashMap<>();

    static {
        for (InterBrokerApiVersion apiVersion : values()) {
            VERSION_MAP.put(apiVersion.version(), apiVersion);
            VERSION_MAP.put(apiVersion.shortVersion, apiVersion);
        }
    }

    InterBrokerApiVersion(int id, String shortVersion, RecordVersion recordVersion) {
        this(id, shortVersion, null, recordVersion);
    }

    InterBrokerApiVersion(int id, String shortVersion, String subVersion, RecordVersion recordVersion) {
        this.id = id;
        this.shortVersion = shortVersion;
        this.subVersion = subVersion;
        this.recordVersion = recordVersion;

    }

    public String version() {
        if (subVersion == null)
            return shortVersion;
        return shortVersion + "-" + subVersion;
    }

    @Override
    public String toString() {
        return version();
    }

    /**
     * Return an `InterBrokerApiVersion` instance for `versionString`, which can be in a variety of formats
     * (e.g. "0.8.0", "0.8.0.x", "0.10.0", "0.10.0-IV1").
     *
     * @throws IllegalArgumentException if `versionString` cannot be mapped to an `ApiVersion`.
     */
    public static InterBrokerApiVersion fromString(String versionString) {
        String[] versionSegments = versionString.split("\\.");
        int numSegments;
        if (versionString.startsWith("0."))
            numSegments = 3;
        else
            numSegments = 2;

        if (versionSegments.length < numSegments)
            throw new IllegalArgumentException("Version " + versionString + " is not a valid version");

        StringBuilder version = new StringBuilder()
                .append(versionSegments[0])
                .append(".").append(versionSegments[1]);

        if (numSegments == 3)
            version.append(".").append(versionSegments[2]);

        InterBrokerApiVersion apiVersion = VERSION_MAP.get(version.toString());
        if (apiVersion == null)
            throw new IllegalArgumentException("Version " + versionString + " is not a valid version");

        return apiVersion;
    }

    public static InterBrokerApiVersion latestVersion() {
        return InterBrokerApiVersion.values()[InterBrokerApiVersion.values().length - 1];
    }

    /**
     * Return the minimum `ApiVersion` that supports `RecordVersion`.
     */
    public static InterBrokerApiVersion minSupportedFor(RecordVersion recordVersion) {
        switch (recordVersion) {
            case V0: return KAFKA_0_8_0;
            case V1: return KAFKA_0_10_0_IV0;
            case V2: return KAFKA_0_11_0_IV0;
            default: throw new IllegalArgumentException("Unhandled version " + recordVersion);
        }
    }

}
