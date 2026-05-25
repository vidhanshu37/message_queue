package com.mq.storage;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.io.Closeable;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;

@Slf4j
public class OffsetIndex implements Closeable{

    // How often we record a position - every INDEX_INTERVAL message
    private static final int INDEX_INTERVAL = 100;

    // In memory mirror of the index for fast lookup
    // Key = offset, Value = byte position in the .log file
    private final TreeMap<Long, Long> index = new java.util.TreeMap<>();

    private final FileChannel channel;
    private int messageCountSinceLastIndex = 0;

    public OffsetIndex(Path indexPath) throws IOException {
        this.channel = FileChannel.open(indexPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );
        loadExistingIndex();
    }

    ///  On startup, read existing index file back into memory.
    private void loadExistingIndex() throws IOException{
        // Each entry : 8bytes for offset + 8 bytes for position = 16 bytes
        ByteBuffer buffer = ByteBuffer.allocate(16);
        channel.position(0);

        while(channel.read(buffer) == 16) {
            buffer.flip();
            long offset = buffer.getLong();
            long position = buffer.getLong();
            index.put(offset, position);
            buffer.clear();
        }
        log.info("Loaded {} index entries from {}", index.size(), channel);
    }

    /**
     * Called after every message write
     * Only actually writes to disk every INDEX_INTERVAL messages
     */
    public void maybeRecord(long offset, long filePosition) throws IOException {
        messageCountSinceLastIndex ++;
        if(messageCountSinceLastIndex >= INDEX_INTERVAL) {
            record (offset, filePosition);
            messageCountSinceLastIndex = 0;
        }
    }

    /**
     * Force write an index entry - used for segment base offset
     */
    public void record(long offset, long filePosition) throws IOException {
        // Write to in-memory index
        index.put(offset, filePosition);

        // Append to index file
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(offset);
        buffer.putLong(filePosition);
        buffer.flip();

        channel.position(channel.size());
        channel.write(buffer);
    }

    /**
     * Find the largest indexed offset that is <= targetOffset.
     * This gives us a file position to start scanning from.
     *
     * TreeMap.floorKey() is O(log N) — exactly why we use TreeMap here.
     */
    public long findNearestPosition(long targetOffset) {
        Long floorOffset = index.floorKey(targetOffset);
        if(floorOffset == null) {
            return 0L;
        }
        return index.get(floorOffset);
    }

    public boolean isEmpty() {
        return index.isEmpty();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
