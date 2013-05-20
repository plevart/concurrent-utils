/*
 * Copyright (C) 2013 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A one-shot lock implementation that can be used to implement lazy initialization with minimum state overhead.
 * A {@code LazyLock} is {@link #LazyLock initialized} with an instance of {@link AtomicIntegerFieldUpdater} and
 * a {@code lockIndex} (a value between 0 and 15) which determines which 2-bit tuple to use in the 32-bit int field
 * designated by the {@code AtomicIntegerFieldUpdater} to maintain initialized / initializing state. One volatile int
 * field can be used for 16 {@link LazyLock}s.<p>
 * {@code LazyLock} can be used with the following idiom:
 * <pre>
public class Container {

    // state0 (2 bits per lazy field, good for 1st 16 lazy fields)
    private volatile int state0;

    // CAS support for state0
    private static final AtomicIntegerFieldUpdater<Container> state0Updater
        = AtomicIntegerFieldUpdater.newUpdater(Container.class, "state0");

    // the lazy val field
    private int lazyVal;

    // lock for lazyVal initialization
    private static final LazyLock<Container> lazyValLock
        = new LazyLock<Container>(state0Updater, 0);

    private int computeLazyVal() {
        // this is the expression used to initialize lazy val
        return 123;
    }

    public int getLazyVal() {
        if (lazyValLock.lock(this)) {
            try {
                return lazyVal = computeLazyVal();
            } finally {
                lazyValLock.unlock(this);
            }
        } else {
            return lazyVal;
        }
    }
 * </pre>
 *
 *
 * @author peter
 */
public class LazyLock<T> {
    private final AtomicIntegerFieldUpdater<T> stateUpdater;
    private static final int state0 = 0; // uninitialized
    private final int
        state1, // initializing - no waiters
        state2, // initializing - at lest one waiter
        state3; // initialized

    /**
     * Construct an instance of {@link LazyLock}.
     *
     * @param stateUpdater the AtomicIntegerFieldUpdater that designates the {@code volatile int} field to
     *                     use to maintain initialized / initializing state
     * @param lockIndex    the index (between 0 and 15) of the 2-bit tuple to use in the 32-bit int field
     */
    public LazyLock(AtomicIntegerFieldUpdater<T> stateUpdater, int lockIndex) {
        if (lockIndex < 0 || lockIndex > 15) {
            throw new IllegalArgumentException("lockIndex (" + lockIndex + ") should be between 0 and 15");
        }
        this.stateUpdater = stateUpdater;
        state1 = 1 << (lockIndex + lockIndex);
        state2 = state1 + state1;
        state3 = state2 + state1;
    }

    /**
     * Attempt to obtain a lock under which single lazy initialization can be performed.
     *
     * @param target the holder of the int "state" field and an object to use for synchronization/notification
     * @return true if lock has been granted and the thread should initialize the structure and {@link #unlock} should
     *         be called or false if the lazy structure is already initialized and should just be used and no
     *         {@link #unlock} called.
     */
    public boolean lock(T target) {
        for (; ; ) {
            int state = stateUpdater.get(target);
            int myState = state & state3;
            if (myState == state3) {
                // already initialized - no locking needed
                return false;
            }
            else if (myState == state0) {
                if (stateUpdater.compareAndSet(target, state, state + state1)) {
                    // lock established, should call unlock after initializing the lazy val
                    return true;
                }
            }
            else if (myState == state1) {
                // we are the 1st waiter
                synchronized (target) {
                    if (stateUpdater.compareAndSet(target, state, state + state1)) {
                        try {
                            target.wait();
                        }
                        catch (InterruptedException e) {}
                    }
                }
            }
            else { // myState == state2
                // we are the 2nd or subsequent waiter
                synchronized (target) {
                    if ((stateUpdater.get(target) & state3) == state2) { // recheck
                        try {
                            target.wait();
                        }
                        catch (InterruptedException e) {}
                    }
                }
            }
        }
    }

    /**
     * Unlock the lock, change it's state to "initialized" and notify any threads waiting for initialization to
     * finish to wake-up and signal the completion of lazy initialization.
     *
     * @param target the holder of the int "state" field and an object to use for synchronization/notification
     * @throws IllegalStateException if trying to unlock but it was not locked
     */
    public void unlock(T target) {
        for (; ; ) {
            int state = stateUpdater.get(target);
            int myState = state & state3;
            if (myState == state1) {
                // still no waiters - just set state to initialized
                if (stateUpdater.compareAndSet(target, state, state + state2)) {
                    return;
                }
            }
            else if (myState == state2) {
                // there are waiters - notify them after changing state to initialized
                synchronized (target) {
                    if (stateUpdater.compareAndSet(target, state, state + state1)) {
                        target.notifyAll();
                        return;
                    }
                }
            }
            else { // myState == state0 || myState == state3
                throw new IllegalStateException("Not locked!");
            }
        }
    }
}
