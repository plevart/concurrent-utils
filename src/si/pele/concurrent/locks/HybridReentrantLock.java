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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

/**
 * A re-entrant mutual exclusion {@link Lock} implementation based on
 * mixed usage of atomic operations (for basic {@link Lock} API) and Java Object monitor locks
 * (for {@link Condition} API).
 *
 * @author peter.levart@gmail.com
 */
public class HybridReentrantLock extends MonitorCondition.Support implements Lock {

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

        volatile Thread thread;
        volatile Waiter next;
        // lockCount is only accessed by single thread (the Waiter.thread)
        int lockCount;

        private Waiter() {
        }

        Waiter(Thread thread) {
            // need not be a volatile write (Waiters are published safely)
            U.putOrderedObject(this, WAITER_THREAD_OFFSET, thread);
        }

        final boolean casThread(Thread oldThread, Thread newTread) {
            return U.compareAndSwapObject(this, WAITER_THREAD_OFFSET, oldThread, newTread);
        }

        final boolean casNext(Waiter oldNext, Waiter newNext) {
            return U.compareAndSwapObject(this, WAITER_NEXT_OFFSET, oldNext, newNext);
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
        // clear interrupted status and throw if it was true
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        Thread ct = Thread.currentThread();
        Waiter h = head;

        if (h != null && ct == h.thread) {

            // nested lock
            h.lockCount++;

        } else {

            Waiter w = new Waiter(ct);
            if (!pushWaiter(h, w)) {
                // haven't got lock without contention...
                do {
                    LockSupport.park();
                    // re-read head after parking
                    h = head;
                    // were we interrupted?
                    if (Thread.interrupted()) {
                        if (w.casThread(ct, null)) {
                            // successfully unregistered from waiting list
                            throw new InterruptedException();
                        } else {
                            // the lock is just being released and we have already been
                            // chosen as the next holder and an unpark has/will be wasted
                            // on us, so we just spin until we got the lock
                            while (w != h) {
                                h = head;
                            }
                            // set-back interrupted status
                            Thread.currentThread().interrupt();
                        }
                    }
                } while (w != h);

                // set current thread back (announcements for unparking clear it)
                w.thread = ct;
            }

            // got lock
            w.lockCount = 1;
        }
    }

    @Override
    public boolean tryLock() {
        Thread ct = Thread.currentThread();
        Waiter h = head;

        if (h == null) {
            Waiter w = new Waiter(ct);
            if (pushWaiter(h, w)) {
                // got lock without contention
                w.lockCount = 1;
                return true;
            } else if (w.casThread(ct, null)) {
                // haven't got lock immediately but successfully unregistered
                return false;
            } else {
                // the lock is just being released and we have already been
                // chosen as the next holder and an unpark has/will be wasted
                // on us, so we just spin until we got the lock
                while (w != h) {
                    h = head;
                }
                // set current thread back (announcements for unparking clear it)
                w.thread = ct;
                // got lock
                w.lockCount = 1;
                return true;
            }
        } else if (ct == h.thread) {
            // nested lock
            h.lockCount++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        // clear interrupted status and throw if it was true
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        long deadline = System.nanoTime() + unit.toNanos(time);

        Thread ct = Thread.currentThread();
        Waiter h = head;

        if (h != null && ct == h.thread) {

            // nested lock
            h.lockCount++;

        } else {

            Waiter w = new Waiter(ct);
            if (!pushWaiter(h, w)) {
                // haven't got lock without contention...
                do {
                    long nanos = deadline - System.nanoTime();
                    boolean timedOut = nanos <= 0L;
                    if (!timedOut) {
                        LockSupport.parkNanos(this, nanos);
                        // re-read head after parking
                        h = head;
                    }
                    // were we interrupted?
                    boolean interrupted = Thread.interrupted();
                    // or timed-out?
                    if (interrupted || timedOut) {
                        if (w.casThread(ct, null)) {
                            // successfully unregistered from waiting list
                            if (interrupted) {
                                throw new InterruptedException();
                            } else { // timed-out
                                return false;
                            }
                        } else {
                            // the lock is just being released and we have already been
                            // chosen as the next holder and an unpark has/will be wasted
                            // on us, so we just spin until we got the lock
                            while (w != h) {
                                h = head;
                            }
                            // set-back interrupted status
                            if (interrupted) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                } while (w != h);

                // set current thread back (announcements for unparking clear it)
                w.thread = ct;
            }

            // got lock
            w.lockCount = 1;
        }

        return true;
    }

    @Override
    public void unlock() {
        release(true);
    }

    private int release(boolean nested) {
        Thread ct = Thread.currentThread();
        Waiter w = head;
        if (w != null && ct == w.thread) {
            int lockDecrement = nested ? 1 : w.lockCount;
            if ((w.lockCount -= lockDecrement) == 0) {
                // find next live waiter and unpark it after transferring the lock to it
                while (true) {
                    Waiter n = w.next;
                    if (n == null) {
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
                            break;
                        }
                    } else {
                        Thread nt = n.thread;
                        if (nt != null && n.casThread(nt, null)) {
                            // successfully found next waiting thread and announced unparking
                            // by cas-ing thread from nt to null -> transfer lock to it
                            head = n;
                            // unpark it
                            LockSupport.unpark(nt);
                            break;
                        } else {
                            // try next in chain...
                            w = n;
                        }
                    }
                }
            }
            return lockDecrement;
        } else {
            throw new IllegalMonitorStateException("Not owner");
        }
    }

    private void acquire(int lockIncrement) {
        Thread ct = Thread.currentThread();
        Waiter h = head;

        if (h != null && ct == h.thread) {

            // nested lock
            h.lockCount += lockIncrement;

        } else {

            Waiter w = new Waiter(ct);
            if (!pushWaiter(h, w)) {
                // haven't got lock without contention...
                boolean interrupted = false;
                do {
                    LockSupport.park();
                    // re-read head after parking
                    h = head;
                    // clear interrupted status but remember it
                    interrupted |= Thread.interrupted();
                } while (w != h);

                if (interrupted) {
                    // set interrupted status if we were interrupted while parking
                    Thread.currentThread().interrupt();
                }

                // set current thread back (announcements for unparking clear it)
                w.thread = ct;
            }

            // got lock
            w.lockCount = lockIncrement;
        }
    }

    /**
     * Push {@code waiter} to the end of waiting list and return true if we
     * managed to push on head (thus we got lock without contention) or
     * false otherwise...
     *
     * @param head   the {@link #head} that has been read recently
     *               (to avoid useless re-read)
     * @param waiter a {@link Waiter} to push to the end of linked list
     * @return true if we got lock (we are at head)
     */
    private boolean pushWaiter(Waiter head, Waiter waiter) {

        while (true) {
            if (head == null) {
                if (casHead(null, waiter)) {
                    tail = waiter;
                    return true;
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
                            return false;
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
    static final long HEAD_OFFSET, WAITER_NEXT_OFFSET, WAITER_THREAD_OFFSET;

    static {
        try {
            Field uf = Unsafe.class.getDeclaredField("theUnsafe");
            uf.setAccessible(true);
            U = (Unsafe) uf.get(null);
            HEAD_OFFSET = U.objectFieldOffset(HybridReentrantLock.class.getDeclaredField("head"));
            WAITER_NEXT_OFFSET = U.objectFieldOffset(Waiter.class.getDeclaredField("next"));
            WAITER_THREAD_OFFSET = U.objectFieldOffset(Waiter.class.getDeclaredField("thread"));
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new Error(e);
        }
    }
}
