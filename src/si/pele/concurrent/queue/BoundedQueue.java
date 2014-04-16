/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.queue;

import java.util.Queue;

/**
 * A {@link Queue} extension which is bounded.
 *
 * @author peter.levart@gmail.com
 */
public interface BoundedQueue<E> extends Queue<E> {

    int capacity();

    default int remainingCapacity() {
        return capacity() - size();
    }
}
