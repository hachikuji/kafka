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
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileLogBufferTest {

    private Record[] records = new Record[] {
            Record.create("abcd".getBytes()),
            Record.create("efgh".getBytes()),
            Record.create("ijkl".getBytes())
    };
    private FileLogBuffer logBuffer;

    @Before
    public void setup() throws IOException {
        this.logBuffer = createLogBuffer(records);
    }

    /**
     * Test that the cached size variable matches the actual file size as we append messages
     */
    @Test
    public void testFileSize() throws IOException {
        assertEquals(logBuffer.channel().size(), logBuffer.sizeInBytes());
        for (int i = 0; i < 20; i++) {
            logBuffer.append(MemoryLogBuffer.withRecords(Record.create("abcd".getBytes())));
            assertEquals(logBuffer.channel().size(), logBuffer.sizeInBytes());
        }
    }

    /**
     * Test that adding invalid bytes to the end of the log doesn't break iteration
     */
    @Test
    public void testIterationOverPartialAndTruncation() throws IOException {
        testPartialWrite(0, logBuffer);
        testPartialWrite(2, logBuffer);
        testPartialWrite(4, logBuffer);
        testPartialWrite(5, logBuffer);
        testPartialWrite(6, logBuffer);
    }

    private void testPartialWrite(int size, FileLogBuffer logBuffer) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (int i = 0; i < size; i++)
            buffer.put((byte) 0);

        buffer.rewind();

        logBuffer.channel().write(buffer);

        // appending those bytes should not change the contents
        TestUtils.checkEquals(Arrays.asList(records).iterator(), logBuffer.records(false));
    }

    /**
     * Iterating over the file does file reads but shouldn't change the position of the underlying FileChannel.
     */
    @Test
    public void testIterationDoesntChangePosition() throws IOException {
        long position = logBuffer.channel().position();
        TestUtils.checkEquals(Arrays.asList(records).iterator(), logBuffer.records(false));
        assertEquals(position, logBuffer.channel().position());
    }

    /**
     * Test a simple append and read.
     */
    @Test
    public void testRead() throws IOException {
        FileLogBuffer read = logBuffer.read(0, logBuffer.sizeInBytes());
        TestUtils.checkEquals(logBuffer.shallowEntries(), read.shallowEntries());

        List<LogEntry> items = shallowEntries(read);
        LogEntry second = items.get(1);

        read = logBuffer.read(second.size(), logBuffer.sizeInBytes());
        assertEquals("Try a read starting from the second message",
                items.subList(1, 3), shallowEntries(read));

        read = logBuffer.read(second.size(), second.size());
        assertEquals("Try a read of a single message starting from the second message",
                Collections.singletonList(second), shallowEntries(read));
    }

    /**
     * Test the MessageSet.searchFor API.
     */
    @Test
    public void testSearch() throws IOException {
        // append a new message with a high offset
        Record lastMessage = Record.create("test".getBytes());
        logBuffer.append(MemoryLogBuffer.withRecords(50L, lastMessage));

        List<LogEntry> entries = shallowEntries(logBuffer);
        int position = 0;

        int message1Size = entries.get(0).size();
        assertEquals("Should be able to find the first message by its offset",
                new FileLogBuffer.LogEntryPosition(0L, position, message1Size),
                logBuffer.searchForOffsetWithSize(0, 0));
        position += message1Size;

        int message2Size = entries.get(1).size();
        assertEquals("Should be able to find second message when starting from 0",
                new FileLogBuffer.LogEntryPosition(1L, position, message2Size),
                logBuffer.searchForOffsetWithSize(1, 0));
        assertEquals("Should be able to find second message starting from its offset",
                new FileLogBuffer.LogEntryPosition(1L, position, message2Size),
                logBuffer.searchForOffsetWithSize(1, position));
        position += message2Size + entries.get(2).size();

        int message4Size = entries.get(3).size();
        assertEquals("Should be able to find fourth message from a non-existant offset",
                new FileLogBuffer.LogEntryPosition(50L, position, message4Size),
                logBuffer.searchForOffsetWithSize(3, position));
        assertEquals("Should be able to find fourth message by correct offset",
                new FileLogBuffer.LogEntryPosition(50L, position, message4Size),
                logBuffer.searchForOffsetWithSize(50,  position));
    }

    /**
     * Test that the message set iterator obeys start and end slicing
     */
    @Test
    public void testIteratorWithLimits() throws IOException {
        LogEntry entry = shallowEntries(logBuffer).get(1);
        long start = logBuffer.searchForOffsetWithSize(1, 0).position;
        int size = entry.size();
        FileLogBuffer slice = logBuffer.read(start, size);
        assertEquals(Collections.singletonList(entry), shallowEntries(slice));
        FileLogBuffer slice2 = logBuffer.read(start, size - 1);
        assertEquals(Collections.emptyList(), shallowEntries(slice2));
    }

    /**
     * Test the truncateTo method lops off messages and appropriately updates the size
     */
    @Test
    public void testTruncate() throws IOException {
        LogEntry entry = shallowEntries(logBuffer).get(0);
        long end = logBuffer.searchForOffsetWithSize(1, 0).position;
        logBuffer.truncateTo((int) end);
        assertEquals(Collections.singletonList(entry), shallowEntries(logBuffer));
        assertEquals(entry.size(), logBuffer.sizeInBytes());
    }

    /**
     * Test that truncateTo only calls truncate on the FileChannel if the size of the
     * FileChannel is bigger than the target size. This is important because some JVMs
     * change the mtime of the file, even if truncate should do nothing.
     */
    @Test
    public void testTruncateNotCalledIfSizeIsSameAsTargetSize() throws IOException {
        FileChannel channelMock = EasyMock.createMock(FileChannel.class);

        EasyMock.expect(channelMock.size()).andReturn(42L).atLeastOnce();
        EasyMock.expect(channelMock.position(42L)).andReturn(null);
        EasyMock.replay(channelMock);

        FileLogBuffer logBuffer = new FileLogBuffer(tempFile(), channelMock, 0, Integer.MAX_VALUE, false);
        logBuffer.truncateTo(42);

        EasyMock.verify(channelMock);
    }

    /**
     * Expect a KafkaException if targetSize is bigger than the size of
     * the FileMessageSet.
     */
    @Test
    public void testTruncateNotCalledIfSizeIsBiggerThanTargetSize() throws IOException {
        FileChannel channelMock = EasyMock.createMock(FileChannel.class);

        EasyMock.expect(channelMock.size()).andReturn(42L).atLeastOnce();
        EasyMock.expect(channelMock.position(42L)).andReturn(null);
        EasyMock.replay(channelMock);

        FileLogBuffer logBuffer = new FileLogBuffer(tempFile(), channelMock, 0, Integer.MAX_VALUE, false);

        try {
            logBuffer.truncateTo(43);
            fail("Should throw KafkaException");
        } catch (KafkaException e) {
            // expected
        }

        EasyMock.verify(channelMock);
    }

    /**
     * see #testTruncateNotCalledIfSizeIsSameAsTargetSize
     */
    @Test
    public void testTruncateIfSizeIsDifferentToTargetSize() throws IOException {
        FileChannel channelMock = EasyMock.createMock(FileChannel.class);

        EasyMock.expect(channelMock.size()).andReturn(42L).atLeastOnce();
        EasyMock.expect(channelMock.position(42L)).andReturn(null).once();
        EasyMock.expect(channelMock.truncate(23L)).andReturn(null).once();
        EasyMock.expect(channelMock.position(23L)).andReturn(null).once();
        EasyMock.replay(channelMock);

        FileLogBuffer logBuffer = new FileLogBuffer(tempFile(), channelMock, 0, Integer.MAX_VALUE, false);
        logBuffer.truncateTo(23);

        EasyMock.verify(channelMock);
    }

    /**
     * Test the new FileMessageSet with pre allocate as true
     */
    @Test
    public void testPreallocateTrue() throws IOException {
        File temp = tempFile();
        FileLogBuffer logBuffer = FileLogBuffer.open(temp, false, 512 * 1024 * 1024, true);
        long position = logBuffer.channel().position();
        int size = logBuffer.sizeInBytes();
        assertEquals(0, position);
        assertEquals(0, size);
        assertEquals(512 * 1024 * 1024, temp.length());
    }

    /**
     * Test the new FileMessageSet with pre allocate as false
     */
    @Test
    public void testPreallocateFalse() throws IOException {
        File temp = tempFile();
        FileLogBuffer set = FileLogBuffer.open(temp, false, 512 * 1024 * 1024, false);
        long position = set.channel().position();
        int size = set.sizeInBytes();
        assertEquals(0, position);
        assertEquals(0, size);
        assertEquals(0, temp.length());
    }

    /**
     * Test the new FileMessageSet with pre allocate as true and file has been clearly shut down, the file will be truncate to end of valid data.
     */
    @Test
    public void testPreallocateClearShutdown() throws IOException {
        File temp = tempFile();
        FileLogBuffer set = FileLogBuffer.open(temp, false, 512 * 1024 * 1024, true);
        set.append(MemoryLogBuffer.withRecords(records));

        int oldPosition = (int) set.channel().position();
        int oldSize = set.sizeInBytes();
        assertEquals(logBuffer.sizeInBytes(), oldPosition);
        assertEquals(logBuffer.sizeInBytes(), oldSize);
        set.close();

        File tempReopen = new File(temp.getAbsolutePath());
        FileLogBuffer setReopen = FileLogBuffer.open(tempReopen, true, 512 * 1024 * 1024, true);
        int position = (int) setReopen.channel().position();
        int size = setReopen.sizeInBytes();

        assertEquals(oldPosition, position);
        assertEquals(oldPosition, size);
        assertEquals(oldPosition, tempReopen.length());
    }

    @Test
    public void testFormatConversionWithPartialMessage() throws IOException {
        LogEntry entry = shallowEntries(logBuffer).get(1);
        long start = logBuffer.searchForOffsetWithSize(1, 0).position;
        int size = entry.size();
        FileLogBuffer slice = logBuffer.read(start, size - 1);
        LogBuffer messageV0 = slice.toMessageFormat(Record.MAGIC_VALUE_V0);
        assertTrue("No message should be there", shallowEntries(messageV0).isEmpty());
        assertEquals("There should be " + (size - 1) + " bytes", size - 1, messageV0.sizeInBytes());
    }

    @Test
    public void testConvertNonCompressedToMagic1() throws IOException {
        List<LogEntry> entries = Arrays.asList(
                LogEntry.create(0L, Record.create(Record.MAGIC_VALUE_V0, Record.NO_TIMESTAMP, "k1".getBytes(), "hello".getBytes())),
                LogEntry.create(2L, Record.create(Record.MAGIC_VALUE_V0, Record.NO_TIMESTAMP, "k2".getBytes(), "goodbye".getBytes())));
        MemoryLogBuffer logBuffer = MemoryLogBuffer.withLogEntries(CompressionType.NONE, entries);

        // Up conversion. In reality we only do down conversion, but up conversion should work as well.
        // up conversion for non-compressed messages
        try (FileLogBuffer fileLogBuffer = FileLogBuffer.open(tempFile())) {
            fileLogBuffer.append(logBuffer);
            fileLogBuffer.flush();
            LogBuffer convertedLogBuffer = fileLogBuffer.toMessageFormat(Record.MAGIC_VALUE_V1);
            verifyConvertedMessageSet(entries, convertedLogBuffer, Record.MAGIC_VALUE_V1);
        }
    }

    @Test
    public void testConvertCompressedToMagic1() throws IOException {
        List<LogEntry> entries = Arrays.asList(
                LogEntry.create(0L, Record.create(Record.MAGIC_VALUE_V0, Record.NO_TIMESTAMP, "k1".getBytes(), "hello".getBytes())),
                LogEntry.create(2L, Record.create(Record.MAGIC_VALUE_V0, Record.NO_TIMESTAMP, "k2".getBytes(), "goodbye".getBytes())));
        MemoryLogBuffer logBuffer = MemoryLogBuffer.withLogEntries(CompressionType.defaultCompressionType(), entries);

        // up conversion for compressed messages
        try (FileLogBuffer fileLogBuffer = FileLogBuffer.open(tempFile())) {
            fileLogBuffer.append(logBuffer);
            fileLogBuffer.flush();
            LogBuffer convertedLogBuffer = fileLogBuffer.toMessageFormat(Record.MAGIC_VALUE_V1);
            verifyConvertedMessageSet(entries, convertedLogBuffer, Record.MAGIC_VALUE_V1);
        }
    }

    @Test
    public void testConvertNonCompressedToMagic0() throws IOException {
        List<LogEntry> entries = Arrays.asList(
                LogEntry.create(0L, Record.create(Record.MAGIC_VALUE_V1, 1L, "k1".getBytes(), "hello".getBytes())),
                LogEntry.create(2L, Record.create(Record.MAGIC_VALUE_V1, 2L, "k2".getBytes(), "goodbye".getBytes())));
        MemoryLogBuffer logBuffer = MemoryLogBuffer.withLogEntries(CompressionType.NONE, entries);

        // down conversion for non-compressed messages
        try (FileLogBuffer fileLogBuffer = FileLogBuffer.open(tempFile())) {
            fileLogBuffer.append(logBuffer);
            fileLogBuffer.flush();
            LogBuffer convertedLogBuffer = fileLogBuffer.toMessageFormat(Record.MAGIC_VALUE_V0);
            verifyConvertedMessageSet(entries, convertedLogBuffer, Record.MAGIC_VALUE_V0);
        }
    }

    @Test
    public void testConvertCompressedToMagic0() throws IOException {
        List<LogEntry> entries = Arrays.asList(
                LogEntry.create(0L, Record.create(Record.MAGIC_VALUE_V1, 1L, "k1".getBytes(), "hello".getBytes())),
                LogEntry.create(2L, Record.create(Record.MAGIC_VALUE_V1, 2L, "k2".getBytes(), "goodbye".getBytes())));
        MemoryLogBuffer logBuffer = MemoryLogBuffer.withLogEntries(CompressionType.defaultCompressionType(), entries);

        // down conversion for compressed messages
        try (FileLogBuffer fileLogBuffer = FileLogBuffer.open(tempFile())) {
            fileLogBuffer.append(logBuffer);
            fileLogBuffer.flush();
            LogBuffer convertedLogBuffer = fileLogBuffer.toMessageFormat(Record.MAGIC_VALUE_V0);
            verifyConvertedMessageSet(entries, convertedLogBuffer, Record.MAGIC_VALUE_V0);
        }
    }

    private void verifyConvertedMessageSet(List<LogEntry> initialEntries, LogBuffer convertedLogBuffer, byte magicByte) {
        int i = 0;
        for (LogEntry logEntry : deepEntries(convertedLogBuffer)) {
            assertEquals("magic byte should be " + magicByte, magicByte, logEntry.record().magic());
            assertEquals("offset should not change", initialEntries.get(i).offset(), logEntry.offset());
            assertEquals("key should not change", initialEntries.get(i).record().key(), logEntry.record().key());
            assertEquals("payload should not change", initialEntries.get(i).record().value(), logEntry.record().value());
            i += 1;
        }
    }

    private static List<LogEntry> shallowEntries(LogBuffer buffer) {
        List<LogEntry> entries = new ArrayList<>();
        Iterator<? extends LogEntry> iterator = buffer.shallowEntries();
        while (iterator.hasNext())
            entries.add(iterator.next());
        return entries;
    }

    private static List<LogEntry> deepEntries(LogBuffer buffer) {
        List<LogEntry> entries = new ArrayList<>();
        Iterator<? extends LogEntry> iterator = buffer.shallowEntries();
        while (iterator.hasNext()) {
            for (LogEntry deepEntry : iterator.next())
                entries.add(deepEntry);
        }
        return entries;
    }

    private FileLogBuffer createLogBuffer(Record ... records) throws IOException {
        FileLogBuffer logBuffer = FileLogBuffer.open(tempFile());
        logBuffer.append(MemoryLogBuffer.withRecords(records));
        logBuffer.flush();
        return logBuffer;
    }

}
