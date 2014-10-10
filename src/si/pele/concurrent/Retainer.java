/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A write-only linked-list of ever larger arrays that just
 * retains objects added to it.
 *
 * @author peter
 */
public final class Retainer {

    private static final class Chunk extends AtomicInteger {
        private static final int MIN_CHUNK_SIZE = 2 << 3;
        private static final int MAX_CHUNK_SIZE = 2 << 30;

        private final Object[] array;
        private final Chunk prev;

        Chunk(Chunk prev) {
            this.array = new Object[(prev == null)
                                    ? MIN_CHUNK_SIZE
                                    : (prev.array.length == MAX_CHUNK_SIZE)
                                      ? MAX_CHUNK_SIZE
                                      : prev.array.length << 1];
            this.prev = prev;
        }

        boolean add(Object element) {
            int i = getAndIncrement();
            if (i < array.length) {
                array[i] = element;
                return true;
            } else {
                return false;
            }
        }
    }

    // current chunk
    private volatile Chunk chunk = new Chunk(null);

    public void add(Object element) {
        Chunk chunk = this.chunk;
        if (!chunk.add(element)) {
            addSlow(element);
        }
    }

    private synchronized void addSlow(Object element) {
        Chunk chunk = this.chunk;
        if (!chunk.add(element)) {
            Chunk newChunk = new Chunk(chunk);
            newChunk.add(element);
            this.chunk = newChunk;
        }
    }
}
