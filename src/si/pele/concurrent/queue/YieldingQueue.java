/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.queue;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A set of default methods that can be inherited by a class extending a
 * {@link Queue} implementation in order to produce a {@link BlockingQueue}.
 * It implements {@link #put(Object)}, {@link #offer(Object, long, TimeUnit)},
 * {@link #take()} and {@link #poll(long, TimeUnit)} using looping with
 * spin/yield-based back-off.
 * It also inherits from {@link DrainableQueue}.
 *
 * @author peter.levart@gmail.com
 * @see DrainableQueue
 */
public interface YieldingQueue<E> extends DrainableQueue<E> {

    /**
     * Number of spins before yielding
     */
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
}
