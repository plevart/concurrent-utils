/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.queue;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;

/**
 * A set of default methods that can be inherited by a class extending a
 * {@link java.util.Queue} implementation in order to produce a {@link BlockingQueue}.
 * It implements {@link #drainTo(Collection)}, {@link #drainTo(Collection, int)}  and
 * {@link #remainingCapacity()} which just returns {@link Integer#MAX_VALUE}.
 * Other {@link BlockingQueue} abstract methods remain unimplemented.
 *
 * @author peter.levart@gmail.com
 * @see YieldingQueue
 */
public interface DrainableQueue<E> extends BlockingQueue<E> {
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
