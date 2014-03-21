/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.locks;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

/**
 * A re-entrant mutual exclusion {@link java.util.concurrent.locks.Lock} implementation based on
 * mixed usage of atomic operations (for basic {@link java.util.concurrent.locks.Lock} API) and Java Object monitor locks
 * (for {@link java.util.concurrent.locks.Condition} API).
 *
 * @author peter.levart@gmail.com
 */
public class HybridReentrantLock2 extends MonitorCondition.Support implements Lock {

    private static final int MAX_SPINS = 100;

    private int lockCount;

    private int getLockCountVolatile() {
        return U.getIntVolatile(this, LOCK_COUNT_OFFSET);
    }

    private void setLockCountVolatile(int newLockCount) {
        U.putIntVolatile(this, LOCK_COUNT_OFFSET, newLockCount);
    }

    private boolean casLockCount(int oldLockCount, int newLockCount) {
        return U.compareAndSwapInt(this, LOCK_COUNT_OFFSET, oldLockCount, newLockCount);
    }

    private Thread owner;

    /**
     * FIFO linked list of threads. The first one (head) has got the lock,
     * the rest are waiting on lock:
     * <pre>
     * head -> Waiter.next -> Waiter.next -> ... -> Waiter.next -> null
     *                                              ^
     *                                        tail--/
     * </pre>
     * Waiter.thread is initialized with owning thread before linking the
     * node to the end of the chain. It can be CAS-ed back to null later either
     * by thread that is releasing the lock and transferring the ownership before
     * un-parking the parked thread or by parking thread that wishes to un-register
     * from waiters list because of interrupt or time-out.
     * <p/>
     * The {@link #INVALIDATED} sentinel value is appended to the end of list when
     * the chain is invalidated. That happens when the last element is popped
     * from the head of the list. The 'head' is then reset to null.
     * This ensures that there is no race between pushing new elements at
     * the end and pop-ing the last element from the head.
     */
    private static class Waiter {
        /**
         * a sentinel value put on the end of invalidated (consumed) chain
         */
        static final Waiter INVALIDATED = new Waiter();

        final Thread thread;
        volatile Waiter next;
        // signal: 0 - ready, -1 - canceled, 1 - signaled
        volatile int signal;

        private Waiter() {
            this.thread = null;
        }

        Waiter(Thread thread) {
            this.thread = thread;
        }

        final boolean casNext(Waiter oldNext, Waiter newNext) {
            return U.compareAndSwapObject(this, WAITER_NEXT_OFFSET, oldNext, newNext);
        }

        final boolean casSignal(int oldSignal, int newSignal) {
            return U.compareAndSwapInt(this, WAITER_SIGNAL_OFFSET, oldSignal, newSignal);
        }
    }

    /**
     * head of waiters list (atomically set with CAS on insertion of 1st Waiter)
     */
    private volatile Waiter head;

    private boolean casHead(Waiter oldHead, Waiter newHead) {
        return U.compareAndSwapObject(this, HEAD_OFFSET, oldHead, newHead);
    }

    // eventual cached tail of waiters list (or not far from real tail).
    // this is just optimization so that we don't traverse the whole list on each push
    private volatile Waiter tail;

    // public API

    @Override
    public void lock() {
        acquire(1);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        // check, clear and throw if interrupted upon entry...
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        Thread ct = Thread.currentThread();
        if (owner == ct) {
            // nested lock
            lockCount++;
            return;
        }

        Waiter h;

        // try to avoid waiting for some loops...
        int spins = MAX_SPINS;
        do {
            if ((h = head) == null && // this is to ensure relative fairness
                // i.e. only attempt to acquire lock without waiting when waiters queue
                // appears to be momentarily empty...
                casLockCount(0, 1)) {
                owner = ct;
                return;
            }
        } while (spins-- > 0);

        // else we wait...
        Waiter w = new Waiter(ct);
        pushWaiter(h, w);
        do {
            LockSupport.park(this);

            if (head == w && // only attempt to acquire if 1st in queue
                casLockCount(0, 1)) {
                owner = ct;
                return;
            }
        } while (!Thread.interrupted());

        // we were interrupted -> try to un-register from waiting list
        if (w.casSignal(0, -1)) {
            // successfully unregistered -> throw
            throw new InterruptedException();
        }

        // else the un-park has/will be wasted on us so we just spin until we get lock
        while (!casLockCount(0, 1)) {}

        assert head == w;
        owner = ct;

        // set interrupted status before returning
        Thread.currentThread().interrupt();
    }

    @Override
    public boolean tryLock() {
        Thread ct = Thread.currentThread();
        if (owner == ct) {
            // nested lock
            lockCount++;
            return true;
        }

        // try to avoid waiting for some loops...
        int spins = MAX_SPINS;
        do {
            if (head == null && // this is to ensure relative fairness
                // i.e. only attempt to acquire lock when waiters queue
                // appears to be momentarily empty...
                casLockCount(0, 1)) {
                owner = ct;
                return true;
            }
        } while (spins-- > 0);

        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        // check, clear and throw if interrupted upon entry...
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        Thread ct = Thread.currentThread();
        if (owner == ct) {
            // nested lock
            lockCount++;
            return true;
        }

        long deadline = System.nanoTime() + unit.toNanos(time);
        Waiter h;

        // try to avoid waiting for some loops...
        int spins = MAX_SPINS;
        do {
            if ((h = head) == null && // this is to ensure relative fairness
                // i.e. only attempt to acquire lock without waiting when waiters queue
                // appears to be momentarily empty...
                casLockCount(0, 1)) {
                owner = ct;
                return true;
            }
        } while (spins-- > 0);

        // else we wait...
        Waiter w = new Waiter(ct);
        pushWaiter(h, w);
        boolean interrupted = false;
        boolean timedOut;
        do {
            long nanos = deadline - System.nanoTime();
            timedOut = nanos <= 0L;

            if (!timedOut) {
                LockSupport.parkNanos(this, nanos);

                if (head == w && // only attempt to acquire if 1st in queue
                    casLockCount(0, 1)) {
                    owner = ct;
                    return true;
                }
                interrupted = Thread.interrupted();
            }

        } while (!timedOut && !interrupted);

        // we were interrupted or timed out -> try to un-register from waiting list
        if (w.casSignal(0, -1)) {
            // successfully unregistered -> throw or return false
            if (interrupted) {
                throw new InterruptedException();
            } else {
                return false;
            }
        }

        // else the un-park has been / will be wasted on us so we just spin until we get lock
        while (!casLockCount(0, 1)) {}

        assert head == w;
        owner = ct;

        // set interrupted status before returning
        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        return true;
    }

    @Override
    public void unlock() {
        release(true);
    }

    private int release(boolean nested) {
        Thread ct = Thread.currentThread();
        if (owner == ct) {
            if (nested && lockCount > 1) {
                // nested unlock
                lockCount--;
                return 1;
            } else {
                // full unlock
                int oldLockCount = lockCount;

                Waiter h0 = head;
                owner = null;
                setLockCountVolatile(0);
                Waiter h1 = head;

                // we are unlocked now, we just have to set the head to next waiter (if necessary) and
                // notify it by un-parking it's thread...

                Waiter h = head;
                if (h != null) {
                    loop:
                    while (true) {
                        // we need to find next waiter and set it on head
                        Waiter w = h;
                        Waiter n = h;
                        if (w.thread == ct) {
                            // skip our Waiter
                            n = w.next;
                        }

                        while (n != null) {
                            if (n.casSignal(0, 1)) {
                                // successfully found next waiting thread and announced un-parking
                                head = n;
                                // unpark it
                                LockSupport.unpark(n.thread);
                                break loop;
                            } else {
                                // already canceled -> try next
                                w = n;
                                n = w.next;
                            }
                        }

                        // no next waiter -> try to invalidate chain and reset head
                        if (w.casNext(null, Waiter.INVALIDATED)) {
                            // set 'tail' to INVALIDATED too; this is not strictly necessary since
                            // pushing threads will eventually overwrite the tail that belongs to
                            // invalidated chain with some valid node, but there might not be any
                            // push in the near future and we want to release any reference to Thread
                            // as soon as possible...
                            // (logically we could also set tail to null, but then we risk some pushing
                            // thread would have to walk the whole chain from head to end until it finds
                            // INVALIDATED sentinel)
                            tail = Waiter.INVALIDATED;
                            // finally reset head (allow pushing threads to create new head)
                            head = null;
                            break loop;
                        }
                    }
                }

                return oldLockCount;
            }
        } else {
            throw new IllegalMonitorStateException("Not owner");
        }
    }

    private void acquire(int lockIncrement) {

        Thread ct = Thread.currentThread();
        if (owner == ct) {
            // nested lock
            lockCount += lockIncrement;
            return;
        }

        Waiter h;

        // try to avoid waiting for some loops...
        int spins = MAX_SPINS;
        do {
            if ((h = head) == null && // this is to ensure relative fairness
                // i.e. only attempt to acquire lock without waiting when waiters queue
                // appears to be momentarily empty...
                casLockCount(0, lockIncrement)) {
                owner = ct;
                return;
            }
        } while (spins-- > 0);

        // else we wait...
        Waiter w = new Waiter(ct);
        pushWaiter(h, w);
        boolean interrupted = false;
        while (true) {
            if (head == w && // only attempt to acquire if 1st in queue
                casLockCount(0, lockIncrement)) {
                owner = ct;
                break;
            }

            // clear interrupted status but remember it
            interrupted |= Thread.interrupted();

            LockSupport.park(this);
        }

        if (interrupted) {
            // set interrupted status if we were interrupted while waiting
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Push {@code waiter} to the end of waiting list.
     *
     * @param head   the {@link #head} that has been read recently
     *               (to avoid useless re-read)
     * @param waiter a {@link Waiter} to push to the end of linked list
     */
    private void pushWaiter(Waiter head, Waiter waiter) {

        retryLoop:
        while (true) {
            if (head == null) {
                if (casHead(null, waiter)) {
                    tail = waiter;
                    break retryLoop;
                }
            } else {
                Waiter t = tail;
                if (t == null) {
                    t = head;
                }
                for (Waiter w = t.next; t != Waiter.INVALIDATED; w = t.next) {
                    if (w == null) {
                        if (t.casNext(null, waiter)) {
                            tail = waiter;
                            break retryLoop;
                        }
                    } else {
                        t = w;
                    }
                }
            }
            // re-read head before next iteration
            head = this.head;
        }
    }

    // MonitorCondition.Support implementation

    int releaseLock() {
        return release(false);
    }

    void regainLock(int lockCount) {
        acquire(lockCount);
    }

    void checkLock() {
        Waiter w = head;
        if (w == null || Thread.currentThread() != w.thread) {
            throw new IllegalMonitorStateException("Not owner");
        }
    }

    // Unsafe support (used in nested classes too, so package-private to avoid generated accessor methods)

    static final Unsafe U;
    static final long LOCK_COUNT_OFFSET, HEAD_OFFSET, WAITER_NEXT_OFFSET, WAITER_SIGNAL_OFFSET;

    static {
        try {
            Field uf = Unsafe.class.getDeclaredField("theUnsafe");
            uf.setAccessible(true);
            U = (Unsafe) uf.get(null);
            LOCK_COUNT_OFFSET = U.objectFieldOffset(HybridReentrantLock2.class.getDeclaredField("lockCount"));
            HEAD_OFFSET = U.objectFieldOffset(HybridReentrantLock2.class.getDeclaredField("head"));
            WAITER_NEXT_OFFSET = U.objectFieldOffset(Waiter.class.getDeclaredField("next"));
            WAITER_SIGNAL_OFFSET = U.objectFieldOffset(Waiter.class.getDeclaredField("signal"));
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new Error(e);
        }
    }
}
