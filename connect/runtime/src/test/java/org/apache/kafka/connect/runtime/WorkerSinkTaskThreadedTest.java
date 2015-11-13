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

package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.MockTime;
import org.apache.kafka.connect.util.ThreadedTest;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest(WorkerSinkTask.class)
@PowerMockIgnore("javax.management.*")
public class WorkerSinkTaskThreadedTest extends ThreadedTest {

    // These are fixed to keep this code simpler. In this example we assume byte[] raw values
    // with mix of integer/string in Connect
    private static final String TOPIC = "test";
    private static final int PARTITION = 12;
    private static final int PARTITION2 = 13;
    private static final int PARTITION3 = 14;
    private static final long FIRST_OFFSET = 45;
    private static final Schema KEY_SCHEMA = Schema.INT32_SCHEMA;
    private static final int KEY = 12;
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private static final String VALUE = "VALUE";
    private static final byte[] RAW_KEY = "key".getBytes();
    private static final byte[] RAW_VALUE = "value".getBytes();

    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
    private static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC, PARTITION2);
    private static final TopicPartition TOPIC_PARTITION3 = new TopicPartition(TOPIC, PARTITION3);
    private static final TopicPartition UNASSIGNED_TOPIC_PARTITION = new TopicPartition(TOPIC, 200);

    private static final Map<String, String> TASK_PROPS = new HashMap<>();
    static {
        TASK_PROPS.put(SinkConnector.TOPICS_CONFIG, TOPIC);
    }

    private ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private Time time;
    @Mock private SinkTask sinkTask;
    private Capture<WorkerSinkTaskContext> sinkTaskContext = EasyMock.newCapture();
    private WorkerConfig workerConfig;
    @Mock private Converter keyConverter;
    @Mock
    private Converter valueConverter;
    private WorkerSinkTask workerTask;
    @Mock private KafkaConsumer<byte[], byte[]> consumer;
    private WorkerSinkTaskThread workerThread;
    private Capture<ConsumerRebalanceListener> rebalanceListener = EasyMock.newCapture();

    private long recordsReturned;

    @SuppressWarnings("unchecked")
    @Override
    public void setup() {
        super.setup();
        time = new MockTime();
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put("internal.value.converter.schemas.enable", "false");
        workerConfig = new StandaloneConfig(workerProps);
        workerTask = PowerMock.createPartialMock(
                WorkerSinkTask.class, new String[]{"createConsumer", "createWorkerThread"},
                taskId, sinkTask, workerConfig, keyConverter, valueConverter, time);

        recordsReturned = 0;
    }

    @Test
    public void testPollsInBackground() throws Exception {
        expectInitializeTask();
        Capture<Collection<SinkRecord>> capturedRecords = expectPolls(1L);
        expectStopTask(10L);
        EasyMock.expect(workerThread.awaitShutdown(EasyMock.anyLong(), EasyMock.<TimeUnit>anyObject())).andReturn(true);

        PowerMock.replayAll();

        workerTask.start(TASK_PROPS);
        workerTask.joinConsumerGroupAndStart();
        for (int i = 0; i < 10; i++) {
            workerThread.iteration();
        }
        workerTask.stop();
        workerTask.awaitStop(Long.MAX_VALUE);
        workerTask.close();

        // Verify contents match expected values, i.e. that they were translated properly. With max
        // batch size 1 and poll returns 1 message at a time, we should have a matching # of batches
        assertEquals(10, capturedRecords.getValues().size());
        int offset = 0;
        for (Collection<SinkRecord> recs : capturedRecords.getValues()) {
            assertEquals(1, recs.size());
            for (SinkRecord rec : recs) {
                SinkRecord referenceSinkRecord
                        = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, KEY, VALUE_SCHEMA, VALUE, FIRST_OFFSET + offset);
                assertEquals(referenceSinkRecord, rec);
                offset++;
            }
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testCommit() throws Exception {
        expectInitializeTask();
        // Make each poll() take the offset commit interval
        Capture<Collection<SinkRecord>> capturedRecords
                = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        expectOffsetFlush(1L, null, null, 0, true);
        expectStopTask(2);
        EasyMock.expect(workerThread.awaitShutdown(EasyMock.anyLong(), EasyMock.<TimeUnit>anyObject())).andReturn(true);

        PowerMock.replayAll();

        workerTask.start(TASK_PROPS);
        workerTask.joinConsumerGroupAndStart();
        // First iteration gets one record
        workerThread.iteration();
        // Second triggers commit, gets a second offset
        workerThread.iteration();
        // Commit finishes synchronously for testing so we can check this immediately
        assertEquals(0, workerThread.commitFailures());
        workerTask.stop();
        workerTask.awaitStop(Long.MAX_VALUE);
        workerTask.close();

        assertEquals(2, capturedRecords.getValues().size());

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitTaskFlushFailure() throws Exception {
        expectInitializeTask();
        Capture<Collection<SinkRecord>> capturedRecords = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        expectOffsetFlush(1L, new RuntimeException(), null, 0, true);
        // Should rewind to last known good positions, which in this case will be the offsets loaded during initialization
        // for all topic partitions
        consumer.seek(TOPIC_PARTITION, FIRST_OFFSET);
        PowerMock.expectLastCall();
        consumer.seek(TOPIC_PARTITION2, FIRST_OFFSET);
        PowerMock.expectLastCall();
        consumer.seek(TOPIC_PARTITION3, FIRST_OFFSET);
        PowerMock.expectLastCall();
        expectStopTask(2);
        EasyMock.expect(workerThread.awaitShutdown(EasyMock.anyLong(), EasyMock.<TimeUnit>anyObject())).andReturn(true);

        PowerMock.replayAll();

        workerTask.start(TASK_PROPS);
        workerTask.joinConsumerGroupAndStart();
        // Second iteration triggers commit
        workerThread.iteration();
        workerThread.iteration();
        assertEquals(1, workerThread.commitFailures());
        assertEquals(false, Whitebox.getInternalState(workerThread, "committing"));
        workerTask.stop();
        workerTask.awaitStop(Long.MAX_VALUE);
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitTaskSuccessAndFlushFailure() throws Exception {
        // Validate that we rewind to the correct offsets if a task's flush method throws an exception

        expectInitializeTask();
        Capture<Collection<SinkRecord>> capturedRecords = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        expectOffsetFlush(1L, null, null, 0, true);
        expectOffsetFlush(2L, new RuntimeException(), null, 0, true);
        // Should rewind to last known committed positions
        consumer.seek(TOPIC_PARTITION, FIRST_OFFSET + 1);
        PowerMock.expectLastCall();
        consumer.seek(TOPIC_PARTITION2, FIRST_OFFSET);
        PowerMock.expectLastCall();
        consumer.seek(TOPIC_PARTITION3, FIRST_OFFSET);
        PowerMock.expectLastCall();
        expectStopTask(2);
        EasyMock.expect(workerThread.awaitShutdown(EasyMock.anyLong(), EasyMock.<TimeUnit>anyObject())).andReturn(true);

        PowerMock.replayAll();

        workerTask.start(TASK_PROPS);
        workerTask.joinConsumerGroupAndStart();
        // Second iteration triggers first commit, third iteration triggers second (failing) commit
        workerThread.iteration();
        workerThread.iteration();
        workerThread.iteration();
        assertEquals(1, workerThread.commitFailures());
        assertEquals(false, Whitebox.getInternalState(workerThread, "committing"));
        workerTask.stop();
        workerTask.awaitStop(Long.MAX_VALUE);
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitConsumerFailure() throws Exception {
        expectInitializeTask();
        Capture<Collection<SinkRecord>> capturedRecords
                = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        expectOffsetFlush(1L, null, new Exception(), 0, true);
        expectStopTask(2);
        EasyMock.expect(workerThread.awaitShutdown(EasyMock.anyLong(), EasyMock.<TimeUnit>anyObject())).andReturn(true);

        PowerMock.replayAll();

        workerTask.start(TASK_PROPS);
        workerTask.joinConsumerGroupAndStart();
        // Second iteration triggers commit
        workerThread.iteration();
        workerThread.iteration();
        // TODO Response to consistent failures?
        assertEquals(1, workerThread.commitFailures());
        assertEquals(false, Whitebox.getInternalState(workerThread, "committing"));
        workerTask.stop();
        workerTask.awaitStop(Long.MAX_VALUE);
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitTimeout() throws Exception {
        expectInitializeTask();
        // Cut down amount of time to pass in each poll so we trigger exactly 1 offset commit
        Capture<Collection<SinkRecord>> capturedRecords
                = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT / 2);
        expectOffsetFlush(2L, null, null, WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT, false);
        expectStopTask(4);
        EasyMock.expect(workerThread.awaitShutdown(EasyMock.anyLong(), EasyMock.<TimeUnit>anyObject())).andReturn(true);

        PowerMock.replayAll();

        workerTask.start(TASK_PROPS);
        workerTask.joinConsumerGroupAndStart();
        // Third iteration triggers commit, fourth gives a chance to trigger the timeout but doesn't
        // trigger another commit
        workerThread.iteration();
        workerThread.iteration();
        workerThread.iteration();
        workerThread.iteration();
        // TODO Response to consistent failures?
        assertEquals(1, workerThread.commitFailures());
        assertEquals(false, Whitebox.getInternalState(workerThread, "committing"));
        workerTask.stop();
        workerTask.awaitStop(Long.MAX_VALUE);
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testAssignmentPauseResume() throws Exception {
        // Just validate that the calls are passed through to the consumer, and that where appropriate errors are
        // converted
        expectInitializeTask();

        expectOnePoll().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                assertEquals(new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3)),
                        sinkTaskContext.getValue().assignment());
                return null;
            }
        });
        EasyMock.expect(consumer.assignment()).andReturn(new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3)));

        expectOnePoll().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                try {
                    sinkTaskContext.getValue().pause(UNASSIGNED_TOPIC_PARTITION);
                    fail("Trying to pause unassigned partition should have thrown an Connect exception");
                } catch (ConnectException e) {
                    // expected
                }
                sinkTaskContext.getValue().pause(TOPIC_PARTITION, TOPIC_PARTITION2);
                return null;
            }
        });
        consumer.pause(UNASSIGNED_TOPIC_PARTITION);
        PowerMock.expectLastCall().andThrow(new IllegalStateException("unassigned topic partition"));
        consumer.pause(TOPIC_PARTITION, TOPIC_PARTITION2);
        PowerMock.expectLastCall();

        expectOnePoll().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                try {
                    sinkTaskContext.getValue().resume(UNASSIGNED_TOPIC_PARTITION);
                    fail("Trying to resume unassigned partition should have thrown an Connect exception");
                } catch (ConnectException e) {
                    // expected
                }

                sinkTaskContext.getValue().resume(TOPIC_PARTITION, TOPIC_PARTITION2);
                return null;
            }
        });
        consumer.resume(UNASSIGNED_TOPIC_PARTITION);
        PowerMock.expectLastCall().andThrow(new IllegalStateException("unassigned topic partition"));
        consumer.resume(TOPIC_PARTITION, TOPIC_PARTITION2);
        PowerMock.expectLastCall();

        expectStopTask(0);
        EasyMock.expect(workerThread.awaitShutdown(EasyMock.anyLong(), EasyMock.<TimeUnit>anyObject())).andReturn(true);

        PowerMock.replayAll();

        workerTask.start(TASK_PROPS);
        workerTask.joinConsumerGroupAndStart();
        workerThread.iteration();
        workerThread.iteration();
        workerThread.iteration();
        workerTask.stop();
        workerTask.awaitStop(Long.MAX_VALUE);
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testRewind() throws Exception {
        expectInitializeTask();
        final long startOffset = 40L;
        final Map<TopicPartition, Long> offsets = new HashMap<>();

        expectOnePoll().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                offsets.put(TOPIC_PARTITION, startOffset);
                sinkTaskContext.getValue().offset(offsets);
                return null;
            }
        });

        consumer.seek(TOPIC_PARTITION, startOffset);
        EasyMock.expectLastCall();

        expectOnePoll().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                Map<TopicPartition, Long> offsets = sinkTaskContext.getValue().offsets();
                assertEquals(0, offsets.size());
                return null;
            }
        });

        expectStopTask(3);
        EasyMock.expect(workerThread.awaitShutdown(EasyMock.anyLong(), EasyMock.<TimeUnit>anyObject())).andReturn(true);

        PowerMock.replayAll();

        workerTask.start(TASK_PROPS);
        workerTask.joinConsumerGroupAndStart();
        workerThread.iteration();
        workerThread.iteration();
        workerTask.stop();
        workerTask.awaitStop(Long.MAX_VALUE);
        workerTask.close();

        PowerMock.verifyAll();
    }

    private void expectInitializeTask() throws Exception {
        PowerMock.expectPrivate(workerTask, "createConsumer").andReturn(consumer);

        workerThread = PowerMock.createPartialMock(WorkerSinkTaskThread.class, new String[]{"start", "awaitShutdown"},
                workerTask, "mock-worker-thread", time,
                workerConfig);
        PowerMock.expectPrivate(workerTask, "createWorkerThread")
                .andReturn(workerThread);
        workerThread.start();
        PowerMock.expectLastCall();

        consumer.subscribe(EasyMock.eq(Arrays.asList(TOPIC)), EasyMock.capture(rebalanceListener));
        PowerMock.expectLastCall();

        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andAnswer(new IAnswer<ConsumerRecords<byte[], byte[]>>() {
            @Override
            public ConsumerRecords<byte[], byte[]> answer() throws Throwable {
                rebalanceListener.getValue().onPartitionsAssigned(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3));
                return ConsumerRecords.empty();
            }
        });
        EasyMock.expect(consumer.position(TOPIC_PARTITION)).andReturn(FIRST_OFFSET);
        EasyMock.expect(consumer.position(TOPIC_PARTITION2)).andReturn(FIRST_OFFSET);
        EasyMock.expect(consumer.position(TOPIC_PARTITION3)).andReturn(FIRST_OFFSET);

        sinkTask.initialize(EasyMock.capture(sinkTaskContext));
        PowerMock.expectLastCall();
        sinkTask.start(TASK_PROPS);
        PowerMock.expectLastCall();
    }

    private void expectStopTask(final long expectedMessages) throws Exception {
        final long finalOffset = FIRST_OFFSET + expectedMessages - 1;

        sinkTask.stop();
        PowerMock.expectLastCall();

        // No offset commit since it happens in the mocked worker thread, but the main thread does need to wake up the
        // consumer so it exits quickly
        consumer.wakeup();
        PowerMock.expectLastCall();

        consumer.close();
        PowerMock.expectLastCall();
    }

    // Note that this can only be called once per test currently
    private Capture<Collection<SinkRecord>> expectPolls(final long pollDelayMs) throws Exception {
        // Stub out all the consumer stream/iterator responses, which we just want to verify occur,
        // but don't care about the exact details here.
        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andStubAnswer(
                new IAnswer<ConsumerRecords<byte[], byte[]>>() {
                    @Override
                    public ConsumerRecords<byte[], byte[]> answer() throws Throwable {
                        // "Sleep" so time will progress
                        time.sleep(pollDelayMs);
                        TopicPartition partition = new TopicPartition(TOPIC, PARTITION);
                        long offset = FIRST_OFFSET + recordsReturned;

                        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
                                Collections.singletonMap(
                                        partition,
                                        Arrays.asList(
                                                new ConsumerRecord<>(TOPIC, PARTITION, offset, RAW_KEY, RAW_VALUE)
                                        )),
                                Collections.singletonMap(partition, offset));
                        recordsReturned++;
                        return records;
                    }
                });
        EasyMock.expect(keyConverter.toConnectData(TOPIC, RAW_KEY)).andReturn(new SchemaAndValue(KEY_SCHEMA, KEY)).anyTimes();
        EasyMock.expect(valueConverter.toConnectData(TOPIC, RAW_VALUE)).andReturn(new SchemaAndValue(VALUE_SCHEMA, VALUE)).anyTimes();
        Capture<Collection<SinkRecord>> capturedRecords = EasyMock.newCapture(CaptureType.ALL);
        sinkTask.put(EasyMock.capture(capturedRecords));
        EasyMock.expectLastCall().anyTimes();
        return capturedRecords;
    }

    private IExpectationSetters<Object> expectOnePoll() {
        // Currently the SinkTask's put() method will not be invoked unless we provide some data, so instead of
        // returning empty data, we return one record. The expectation is that the data will be ignored by the
        // response behavior specified using the return value of this method.
        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andAnswer(
                new IAnswer<ConsumerRecords<byte[], byte[]>>() {
                    @Override
                    public ConsumerRecords<byte[], byte[]> answer() throws Throwable {
                        // "Sleep" so time will progress
                        time.sleep(1L);
                        TopicPartition partition = new TopicPartition(TOPIC, PARTITION);
                        long offset = FIRST_OFFSET + recordsReturned;

                        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
                                Collections.singletonMap(
                                        partition,
                                        Arrays.asList(
                                                new ConsumerRecord<>(TOPIC, PARTITION, offset, RAW_KEY, RAW_VALUE)
                                        )),
                                Collections.singletonMap(partition, offset));
                        recordsReturned++;
                        return records;
                    }
                });
        EasyMock.expect(keyConverter.toConnectData(TOPIC, RAW_KEY)).andReturn(new SchemaAndValue(KEY_SCHEMA, KEY));
        EasyMock.expect(valueConverter.toConnectData(TOPIC, RAW_VALUE)).andReturn(new SchemaAndValue(VALUE_SCHEMA, VALUE));
        sinkTask.put(EasyMock.anyObject(Collection.class));
        return EasyMock.expectLastCall();
    }

    private Capture<OffsetCommitCallback> expectOffsetFlush(final long expectedMessages,
                                                              final RuntimeException flushError,
                                                              final Exception consumerCommitError,
                                                              final long consumerCommitDelayMs,
                                                              final boolean invokeCallback)
            throws Exception {
        final long finalOffset = FIRST_OFFSET + expectedMessages;

        // All assigned partitions will have offsets committed, but we've only processed messages/updated offsets for one
        final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        offsetsToCommit.put(TOPIC_PARTITION, new OffsetAndMetadata(finalOffset));
        offsetsToCommit.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        offsetsToCommit.put(TOPIC_PARTITION3, new OffsetAndMetadata(FIRST_OFFSET));
        sinkTask.flush(offsetsToCommit);
        IExpectationSetters<Object> flushExpectation = PowerMock.expectLastCall();
        if (flushError != null) {
            flushExpectation.andThrow(flushError).once();
            return null;
        }

        final Capture<OffsetCommitCallback> capturedCallback = EasyMock.newCapture();
        consumer.commitAsync(EasyMock.eq(offsetsToCommit),
                EasyMock.capture(capturedCallback));
        PowerMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                time.sleep(consumerCommitDelayMs);
                if (invokeCallback)
                    capturedCallback.getValue().onComplete(offsetsToCommit, consumerCommitError);
                return null;
            }
        });
        return capturedCallback;
    }

}
