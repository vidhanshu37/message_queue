package com.mq.storage;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class LogSegment implements Closeable {
    // Roll to a new segment after this size (1 GB in prod, small for testing)
    public static final long MAX_SEGMENT_SIZE = 1024L * 1024 * 1024;

    @Getter
    private final long baseOffset;

    private final FileChannel writeChannel;
    private final FileChannel readChannel;
    private final OffsetIndex offsetIndex;

    @Getter
    private long nextOffset; // next offset to be assigned

    public LogSegment(Path logPath, Path indexPath, long baseOffset) throws IOException {
        this.baseOffset = baseOffset;
        this.nextOffset = baseOffset;

        this.writeChannel = FileChannel.open(logPath,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE
        );

        this.readChannel = FileChannel.open(logPath,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE
        );

        this.offsetIndex = new OffsetIndex(indexPath);

        // If segment already has data (restart scenario), recover nextOffset
        recoverNextOffset();
    }

    /**
     * Scan existing log to find the true nextOffset after a restart
     * Without this, we re-use offsets and corrupt the log
     */
    private void recoverNextOffset() throws IOException {
        if (readChannel.size() == 0) return; // empty segment, start with baseOffset

        readChannel.position(0);
        ByteBuffer lengthBuf = ByteBuffer.allocate(12); // offset(8) + 4 bytes for payload length
        long recoveredOffset = baseOffset;

        while (true) {
            lengthBuf.clear();
            int byteRead = readChannel.read(lengthBuf);
            if (byteRead < 12) break; // reached end of log

            lengthBuf.flip();
            recoveredOffset = lengthBuf.getLong();
            int payloadLength = lengthBuf.getInt();

            // skip over payload bytes
            readChannel.position(readChannel.position() + payloadLength);

        }

        this.nextOffset = recoveredOffset + 1;
        log.info("Segment recovered, nextOffset = {}", nextOffset);
    }

    /**
     *
     * Append a message to the log segment, returning offset assigned to this message
     * Format: [8 bytes offset][4 bytes payload length][N bytes payload]
     */
    public synchronized long append(byte[] payload) throws IOException {
        long messageOffset = nextOffset;
        long writePosition = writeChannel.size();

        // Build binary record
        ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + payload.length);
        buffer.putLong(messageOffset);
        buffer.putInt(payload.length); // length prefix
        buffer.put(payload); // actual data
        buffer.flip();

        writeChannel.write(buffer);

        // Update in-memory and on-disk index
        offsetIndex.maybeRecord(messageOffset, writePosition);

        nextOffset++;
        return messageOffset;
    }

    /**
     * Read message starting at startOffset, up to maxBytes total
     * <p>
     * 1. Ask the index for the nearest file position <= startOffset
     * 2. Scan forward from there, skipping messages before startOffset
     * 3. Collect messages until maxBytes is reached
     */
    public List<MessageRecord> read(long startOffset, int maxBytes) throws IOException {
        List<MessageRecord> results = new ArrayList<>();
        long filePosition = offsetIndex.findNearestPosition(startOffset);

        readChannel.position(filePosition);
        int byteRead = 0;

        ByteBuffer headerBuf = ByteBuffer.allocate(12); // offset(8) + 4 bytes for payload length

        while(byteRead < maxBytes) {
            headerBuf.clear();
            int n = readChannel.read(headerBuf);
            if(n < 12) break; // end of file

            headerBuf.flip();
            long msgOffset = headerBuf.getLong();
            int payloadLength = headerBuf.getInt();

            ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLength);
            readChannel.read(payloadBuf);
            payloadBuf.flip();

            byte[] payload = new byte[payloadLength];
            payloadBuf.get(payload);

            // Skip message that come before our target offset
            if(msgOffset < startOffset) continue;

            results.add(new MessageRecord(msgOffset, payload));
            byteRead += (12 + payloadLength);
        }

        return results;
    }

    public boolean isFull() throws IOException {
        return writeChannel.size() >= MAX_SEGMENT_SIZE;
    }

    public long size() throws IOException {
        return writeChannel.size();
    }

    @Override
    public void close() throws IOException {
        writeChannel.close();
        readChannel.close();
        offsetIndex.close();
    }
}
