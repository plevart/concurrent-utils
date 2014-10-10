/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

/**
 * A write-only list that just retains objects added to it.
 *
 * @author peter
 */
public final class Retainer extends AtomicInteger {

    private static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;

    private Object[] array = new Object[16];

    private final StampedLock lock = new StampedLock();

    public void add(Object element) {

        int i = getAndIncrement();

        if (i >= MAX_ARRAY_LENGTH || i < 0 /* overflow */) {
            getAndDecrement();
            throw new OutOfMemoryError("Max. capacity exceeded");
        }

        long stamp = lock.tryOptimisticRead();
        Object[] a = array;
        if (i < a.length) {
            a[i] = element;
            if (lock.validate(stamp)) { // OK, fast-path done
                return;
            } else { // some thread is resizing the array - wait for it
                stamp = lock.readLock();
                try {
                    // re-check
                    a = array;
                    if (i < a.length) {
                        a[i] = element;
                        return;
                    }
                } finally {
                    lock.unlockRead(stamp);
                }
            }
        }

        // resize the array
        stamp = lock.writeLock();
        try {
            // re-check
            a = array;
            if (i < a.length) {
                a[i] = element;
                return;
            }

            int newLength = a.length;
            do {
                newLength = (newLength < MAX_ARRAY_LENGTH / 2)
                            ? newLength * 2
                            : MAX_ARRAY_LENGTH;
            } while (newLength <= i);

            array = a = Arrays.copyOf(a, newLength);
            a[i] = element;
        } finally {
            lock.unlockWrite(stamp);
        }
    }
}
