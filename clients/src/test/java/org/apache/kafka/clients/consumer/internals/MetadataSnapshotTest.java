/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MetadataSnapshotTest {

    @Test
    public void hashIdentity() {
        SortedMap<String, Integer> partitionsPerTopic = new TreeMap<>();
        partitionsPerTopic.put("foo", 25);
        partitionsPerTopic.put("bar", 50);

        MetadataSnapshot snapshot1 = new MetadataSnapshot(partitionsPerTopic);
        MetadataSnapshot snapshot2 = new MetadataSnapshot(partitionsPerTopic);

        assertTrue(Arrays.equals(snapshot1.hash(), snapshot2.hash()));
    }

    @Test
    public void hashInequality() {
        SortedMap<String, Integer> partitionsPerTopic1 = new TreeMap<>();
        partitionsPerTopic1.put("foo", 25);
        partitionsPerTopic1.put("bar", 50);
        MetadataSnapshot snapshot1 = new MetadataSnapshot(partitionsPerTopic1);

        SortedMap<String, Integer> partitionsPerTopic2 = new TreeMap<>();
        partitionsPerTopic2.put("foo", 26);
        partitionsPerTopic2.put("bar", 50);
        MetadataSnapshot snapshot2 = new MetadataSnapshot(partitionsPerTopic2);

        assertFalse(Arrays.equals(snapshot1.hash(), snapshot2.hash()));
    }

    @Test
    public void subset() {
        SortedMap<String, Integer> partitionsPerTopic1 = new TreeMap<>();
        partitionsPerTopic1.put("foo", 25);
        partitionsPerTopic1.put("bar", 50);
        MetadataSnapshot snapshot1 = new MetadataSnapshot(partitionsPerTopic1);

        SortedMap<String, Integer> partitionsPerTopic2 = new TreeMap<>();
        partitionsPerTopic2.put("foo", 26);
        partitionsPerTopic2.put("bar", 50);
        MetadataSnapshot snapshot2 = new MetadataSnapshot(partitionsPerTopic2);

        assertFalse(Arrays.equals(
                snapshot1.subset(Collections.singleton("foo")).hash(),
                snapshot2.subset(Collections.singleton("foo")).hash()));

        assertTrue(Arrays.equals(
                snapshot1.subset(Collections.singleton("bar")).hash(),
                snapshot2.subset(Collections.singleton("bar")).hash()));

    }




}
