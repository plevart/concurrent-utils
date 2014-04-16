/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.queues;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A set of default methods that can be inherited by a class extending a
 * {@link Queue} implementation in order to produce a {@link BlockingQueue}.
 * It also implements {@link #drainTo} methods and {@link #remainingCapacity()}
 * which just returns {@link Integer#MAX_VALUE}.
 * Waiting is performed using spin/yield-based back-off loops.
 *
 * @author peter.levart@gmail.com
 */
public interface YieldingQueue<E> extends BlockingQueue<E> {

    /** Number of spins before yielding */
    int SPINS = 5;

    static int backoff(int c) {
        if (c < SPINS) {
            return c + 1;
        } else {
            Thread.yield();
            return c;
        }
    }

    @Override
    default void put(E e) throws InterruptedException {
        int c = 0;
        while (!offer(e)) {
            if (Thread.interrupted()) throw new InterruptedException();
            c = backoff(c);
        }
    }

    @Override
    default boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        int c = 0;
        while (!offer(e)) {
            if (Thread.interrupted()) throw new InterruptedException();
            if (System.nanoTime() >= deadline) return false;
            c = backoff(c);
        }
        return true;
    }

    @Override
    default E take() throws InterruptedException {
        int c = 0;
        E e;
        while ((e = poll()) == null) {
            if (Thread.interrupted()) throw new InterruptedException();
            c = backoff(c);
        }
        return e;
    }

    @Override
    default E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        int c = 0;
        E e;
        while ((e = poll()) == null) {
            if (Thread.interrupted()) throw new InterruptedException();
            if (System.nanoTime() >= deadline) return null;
            c = backoff(c);
        }
        return e;
    }

    @Override
    default int drainTo(Collection<? super E> c) {
        E e;
        int n = 0;
        while ((e = poll()) != null) {
            c.add(e);
            n++;
        }
        return n;
    }

    @Override
    default int drainTo(Collection<? super E> c, int maxElements) {
        E e;
        int n = 0;
        while (n < maxElements && (e = poll()) != null) {
            c.add(e);
            n++;
        }
        return n;
    }

    @Override
    default int remainingCapacity() {
        return Integer.MAX_VALUE;
    }
}
