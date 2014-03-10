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

    private volatile Thread owner;

    private boolean casOwner(Thread oldOwner, Thread newOwner) {
        return U.compareAndSwapObject(this, OWNER_OFFSET, oldOwner, newOwner);
    }

    // lockCount is only accessed while holding the lock (owner == Thread.currentThread)
    // so need not be declared volatile.
    private int lockCount;

    /**
     * FIFO linked list of threads waiting on lock:
     * <p/>
     * head -> Waiter.next -> Waiter.next -> ... -> Waiter.next -> null
     * ^
     * tail--
     * <p/>
     * Waiter.thread is initialized with waiting thread before linking the
     * node to the end of the chain. It can be reset
     * to null later if the waiting thread is interrupted or times out.
     * <p/>
     * The 'tail' points to eventual last node of linked list.
     * It can happen (because of race) that it points to some node not far
     * from last node.
     * <p/>
     * The INVALIDATED sentinel value is appended to the end of list when
     * the chain is invalidated. That happens when the last element is popped
     * from the head of the list. The 'head' is reset to null in this case.
     */
    private static class Waiter {
        // a sentinel value put on the end of invalidated chain
        static final Waiter INVALIDATED = new Waiter();

        volatile Thread thread;
        volatile Waiter next;

        private Waiter() {
        }

        Waiter(Thread thread) {
            // need not be a volatile write (we publish Waiters safely)
            U.putOrderedObject(this, WAITER_THREAD_OFFSET, thread);
        }

        final boolean casNext(Waiter oldNext, Waiter newNext) {
            return U.compareAndSwapObject(this, WAITER_NEXT_OFFSET, oldNext, newNext);
        }
    }

    // head of waiters list (atomically updated with CAS)
    private volatile Waiter head;

    private boolean casHead(Waiter oldHead, Waiter newHead) {
        return U.compareAndSwapObject(this, HEAD_OFFSET, oldHead, newHead);
    }

    // eventual cached tail of waiters list (or not far from real tail).
    // this is just optimization so that we don't traverse the whole list on each push
    private volatile Waiter tail;

    // push

    private Waiter pushWaiter(Thread thread) {
        Waiter waiter = new Waiter(thread);

        loop:
        while (true) {
            Waiter first = head;
            if (first == null) {
                if (casHead(null, waiter)) {
                    break;
                }
            } else {
                Waiter last = tail;
                if (last == null) {
                    last = first;
                }
                for (Waiter w = last.next; last != Waiter.INVALIDATED; w = last.next) {
                    if (w == null) {
                        if (last.casNext(null, waiter)) {
                            break loop;
                        }
                    } else {
                        last = w;
                    }
                }
            }
        }

        tail = waiter;
        return waiter;
    }

    // pop will only be called by owner of the lock

    private Thread popWaiter() {
        Thread t;
        Waiter h = head;
        if (h != null) {
            // skip de-registered (interrupted and/or timed-out) waiters
            Waiter w = h;
            Waiter n;
            do {
                t = w.thread;
                if (t == null && (n = w.next) != null) {
                    w = n;
                }
            } while (t == null && n != null);
            // have we got a waiting victim?
            if (t != null) {
                assert w != null;
                // yes -> shorten the chain
                while (true) {
                    Waiter n = w.next;
                    if (n == null) {
                        // in case new chain is now empty, we must 1st attempt
                        // to append an INVALIDATED sentinel node to atomically declare the
                        // chain as invalidated (pushing threads will re-try when this
                        // sentinel is found at the end of the chain)
                        if (w.casNext(null, Waiter.INVALIDATED)) {
                            // set 'tail' to INVALIDATED too (this is not strictly necessary since
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
                        // in case the chain is non-empty, we just set the head
                        // (reminder: popWaiter is only called by the thread holding
                        // the lock and when head is found != null, pushing threads are not updating it
                        // ant more, so no CAS is needed here)
                        head = n;
                        break;
                    }
                }
            }
        } else {
            t = null;
        }

        return t;
    }

    // public API

    @Override
    public void lock() {
        Thread ct = Thread.currentThread();
        Waiter waiter = null;
        boolean interrupted = false;

        while (true) {
            Thread ot = owner;
            if (ot == null) {
                if (casOwner(null, ct)) {
                    lockCount = 1;
                    if (waiter != null) {
                        // de-register from waiters list so we don't consume any more signals
                        waiter.thread = null;
                    }
                    break;
                }
            } else if (ot == ct) {
                // nested lock
                lockCount++;
                assert waiter == null;
                break;
            } else if (waiter == null) {
                waiter = pushWaiter(ct);
                // re-try locking after pushing waiter on the list and before parking because
                // we may have missed a signal...
            } else {
                LockSupport.park(this);
                // clear and remember interrupted status because we may park again later...
                interrupted |= Thread.interrupted();
                // de-register from waiters list so we don't consume any more signals
                // before re-trying the lock
                waiter.thread = null;
                waiter = null;
                // we are about to obtain the lock
                // or push us on waiters list again on next iteration...
            }
        }

        if (interrupted) {
            // set interrupted status if it was cleared before
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        Thread ct = Thread.currentThread();
        Waiter waiter = null;
        boolean interrupted = false;

        while (true) {
            Thread ot = owner;
            if (ot == null) {
                if (casOwner(null, ct)) {
                    lockCount = 1;
                    if (waiter != null) {
                        // de-register from waiters list so we don't consume any more signals
                        waiter.thread = null;
                    }
                    break;
                }
            } else if (ot == ct) {
                // nested lock
                lockCount++;
                assert waiter == null;
                break;
            } else if (interrupted) {
                // we already de-registered from waiters list before re-trying the lock
                assert waiter == null;
                throw new InterruptedException();
            } else if (waiter == null) {
                waiter = pushWaiter(ct);
                // re-try locking after pushing waiter on the list and before parking because
                // we may have missed a signal...
            } else {
                assert !interrupted && waiter != null;
                // no interrupted status pending yet, so we park...
                LockSupport.park(this);
                // clear and remember interrupted status but re-try locking after first
                // de-registering from waiters list so that we don't attract and consume a signal
                // but throw InterruptedException...
                interrupted = Thread.interrupted();
                // de-register from waiters list so we don't consume any more signals
                // before re-trying the lock
                waiter.thread = null;
                waiter = null;
                // we are about to obtain the lock or throw InterruptedException
                // or push us on waiters list again on next iteration...
            }
        }

        if (interrupted) {
            // set interrupted status if it has been detected and we nevertheless
            // obtained the lock...
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean tryLock() {
        Thread ct = Thread.currentThread();
        Thread ot = owner;
        if (ot == null) {
            if (casOwner(null, ct)) {
                lockCount = 1;
                return true;
            }
        } else if (ot == ct) {
            // nested lock
            lockCount++;
            return true;
        }
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(time);
        Thread ct = Thread.currentThread();
        Waiter waiter = null;
        boolean interrupted = false;
        boolean timedOut = false;

        while (true) {
            Thread ot = owner;
            if (ot == null) {
                if (casOwner(null, ct)) {
                    lockCount = 1;
                    // reset flag in case we timed out but obtained the lock nevertheless
                    timedOut = false;
                    break;
                }
            } else if (ot == ct) {
                // nested lock (this is only possible on 1st loop iteration - we never retry nested lock)
                assert waiter == null && !timedOut;
                lockCount++;
                break;
            } else if (interrupted) {
                // we already de-registered from waiters list before re-trying the lock
                assert waiter == null;
                throw new InterruptedException();
            } else if (timedOut) {
                // we already de-registered from waiters list before re-trying the lock (or never registered)
                assert waiter == null;
                break;
            } else if (waiter == null) {
                if (time > 0L) {
                    // we only register on the waiters list if timeout > 0
                    waiter = pushWaiter(ct);
                    // re-try locking after pushing waiter on the list and before parking because
                    // we may have missed a signal...
                } else {
                    // time-out without waiting
                    timedOut = true;
                    break;
                }
            } else {
                assert !interrupted && !timedOut && waiter != null;
                long nanos = deadline - System.nanoTime();
                if (nanos > 0) {
                    LockSupport.parkNanos(this, nanos);
                } else {
                    // timed-out
                    timedOut = true;
                }
                // clear and remember interrupted status but re-try locking after first
                // de-registering from waiters list so that we don't attract and consume a signal
                // but throw InterruptedException or time-out...
                interrupted = Thread.interrupted();
                // de-register from waiters list so we don't consume any more signals
                // before re-trying the lock
                waiter.thread = null;
                waiter = null;
                // we are about to obtain the lock or throw InterruptedException or time-out on next
                // iteration...
            }
        }

        if (waiter != null) {
            assert !timedOut;
            // de-register from waiters list so we don't consume any more signals
            waiter.thread = null;
        }

        if (interrupted) {
            assert !timedOut;
            // set interrupted status if it was cleared before and we nevertheless
            // obtained the lock...
            Thread.currentThread().interrupt();
        }

        return !timedOut;
    }

    @Override
    public void unlock() {
        Thread ct = Thread.currentThread();
        Thread ot = owner;
        if (ot == ct) {
            if (--lockCount == 0) {
                // 1st release the lock
                owner = null;
                // then attempt to unpark the 1st waiter (if there is one)
                Thread t = popWaiter();
                if (t != null) {
                    LockSupport.unpark(t);
                }
            }
        } else {
            throw new IllegalMonitorStateException(
                "Current owner is: " + ot +
                ", not: " + ct
            );
        }
    }

    // MonitorCondition.Support implementation

    int releaseLock() {
        Thread ct = Thread.currentThread();
        Thread ot = owner;
        if (ot == ct) {
            // 1st take the sample of lockCount and reset it
            int lc = lockCount;
            lockCount = 0;
            // then release the lock
            owner = null;
            // then attempt to unpark the 1st waiter (if there is one)
            Thread t = popWaiter();
            if (t != null) {
                LockSupport.unpark(t);
            }
            // return lock count
            return lc;
        } else {
            throw new IllegalMonitorStateException(
                "Current owner is: " + ot +
                ", not: " + ct
            );
        }
    }

    void regainLock(int lockCount) {
        assert lockCount > 0;
        Thread ct = Thread.currentThread();
        Waiter waiter = null;
        boolean interrupted = false;

        while (true) {
            Thread ot = owner;
            if (ot == null) {
                if (casOwner(null, ct)) {
                    this.lockCount = lockCount;
                    if (waiter != null) {
                        // invalidate waiter entry so we don't consume any more signals
                        waiter.thread = null;
                    }
                    break;
                }
            } else if (waiter == null) {
                waiter = pushWaiter(ct);
                // re-try locking after pushing waiter on the list and before parking because
                // we may have missed a signal...
            } else {
                LockSupport.park(this);
                // clear and remember interrupted status because we may park again later...
                interrupted |= Thread.interrupted();
            }
        }

        if (interrupted) {
            // set interrupted status if it was cleared before
            Thread.currentThread().interrupt();
        }
    }

    void checkLock() {
        if (owner != Thread.currentThread()) {
            throw new IllegalMonitorStateException(
                "Current owner is: " + owner +
                ", not: " + Thread.currentThread()
            );
        }
    }

    // Unsafe support (used in nested classes too, so package-private to avoid generated accessor methods)

    static final Unsafe U;
    static final long OWNER_OFFSET, HEAD_OFFSET, WAITER_NEXT_OFFSET, WAITER_THREAD_OFFSET;

    static {
        try {
            Field uf = Unsafe.class.getDeclaredField("theUnsafe");
            uf.setAccessible(true);
            U = (Unsafe) uf.get(null);
            OWNER_OFFSET = U.objectFieldOffset(HybridReentrantLock.class.getDeclaredField("owner"));
            HEAD_OFFSET = U.objectFieldOffset(HybridReentrantLock.class.getDeclaredField("head"));
            WAITER_NEXT_OFFSET = U.objectFieldOffset(Waiter.class.getDeclaredField("next"));
            WAITER_THREAD_OFFSET = U.objectFieldOffset(Waiter.class.getDeclaredField("thread"));
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new Error(e);
        }
    }
}
