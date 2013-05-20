/*
 * Written by Peter Levart <peter.levart@gmail.com>
 * and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
package si.pele.concurrent;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author peter
 */
public class LazyLock<T> {
    private final AtomicIntegerFieldUpdater<T> stateUpdater;
    private static final int state0 = 0; // uninitialized
    private final int
        state1, // initializing - no waiters
        state2, // initializing - at lest one waiter
        state3; // initialized

    public LazyLock(AtomicIntegerFieldUpdater<T> stateUpdater, int lockIndex) {
        if (lockIndex < 0 || lockIndex > 15) {
            throw new IllegalArgumentException("lockIndex (" + lockIndex + ") should be between 0 and 15");
        }
        this.stateUpdater = stateUpdater;
        state1 = 1 << (lockIndex + lockIndex);
        state2 = state1 + state1;
        state3 = state2 + state1;
    }

    public boolean lock(T target) {
        for (; ; ) {
            int state = stateUpdater.get(target);
            int myState = state & state3;
            if (myState == state3) {
                // already initialized - no locking needed
                return false;
            } else if (myState == state0) {
                if (stateUpdater.compareAndSet(target, state, state + state1)) {
                    // lock established, should call unlock after initializing the lazy val
                    return true;
                }
            } else if (myState == state1) {
                // we are the 1st waiter
                synchronized (target) {
                    if (stateUpdater.compareAndSet(target, state, state + state1)) {
                        try {
                            target.wait();
                        } catch (InterruptedException e) {}
                    }
                }
            } else { // myState == state2
                // we are the 2nd or subsequent waiter
                synchronized (target) {
                    if ((stateUpdater.get(target) & state3) == state2) { // recheck
                        try {
                            target.wait();
                        } catch (InterruptedException e) {}
                    }
                }
            }
        }
    }

    public void unlock(T target) {
        for (; ; ) {
            int state = stateUpdater.get(target);
            int myState = state & state3;
            if (myState == state1) {
                // still no waiters - just set state to initialized
                if (stateUpdater.compareAndSet(target, state, state + state2)) {
                    return;
                }
            } else if (myState == state2) {
                // there are waiters - notify them after changing state to initialized
                synchronized (target) {
                    if (stateUpdater.compareAndSet(target, state, state + state1)) {
                        target.notifyAll();
                        return;
                    }
                }
            } else { // myState == state0 || myState == state3
                throw new IllegalStateException("Not locked!");
            }
        }
    }
}
