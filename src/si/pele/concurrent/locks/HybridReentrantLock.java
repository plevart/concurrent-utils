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

    private int lockCount;

    private static class Waiter {
        Thread thread;
        volatile Waiter next;

        Waiter(Thread thread) {
            this.thread = thread;
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

    // eventual cached tail of waiters list
    // (optimization so that we don't traverse the whole list on each push)
    private volatile Waiter tail;

    // a sentinel Waiter value put on tail of invalidated chain
    private static final Waiter INVALIDATED = new Waiter(null);

    // internal implementation

    private Waiter pushWaiter(Thread thread) {
        Waiter waiter = new Waiter(thread);

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
                for (Waiter w = last.next; w != null; w = w.next) {
                    last = w;
                }
                if (last != INVALIDATED && last.casNext(null, waiter)) {
                    break;
                }
            }
        }

        tail = waiter;
        return waiter;
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
                        // invalidate waiter entry so we don't consume any more signals
                        // TODO: proper synchronization with signal()
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
                        // invalidate waiter entry so we don't consume any more signals
                        // TODO: proper synchronization with signal()
                        waiter.thread = null;
                        // waiter = null; // don't really need this since we are exiting the method anyway
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
                // interrupted = false; // don't really need this since we are exiting the method anyway
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
                if (interrupted && waiter != null) {
                    // de-register from waiters list so we don't consume any more signals
                    // before re-trying the lock
                    // TODO: proper synchronization with signal()
                    waiter.thread = null;
                    waiter = null;
                    // we are about to obtain the lock or throw InterruptedException on next
                    // iteration...
                }
            }
        }

        if (interrupted) {
            // set interrupted status if it was cleared before and we nevertheless
            // obtained the lock...
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public synchronized boolean tryLock() {
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
        synchronized (this) {
            while (true) {
                if (owner == null) {
                    owner = Thread.currentThread();
                    lockCount = 1;
                    return true;
                } else if (owner == Thread.currentThread()) {
                    lockCount++;
                    return true;
                } else {
                    long nanos = deadline - System.nanoTime();
                    if (nanos <= 0) {
                        return false;
                    }
                    long millis = nanos / 1000_000L;
                    nanos -= millis * 1000_000L;
                    wait(millis, (int) nanos);
                }
            }
        }
    }

    @Override
    public synchronized void unlock() {
        if (owner == Thread.currentThread()) {
            if (--lockCount == 0) {
                owner = null;
                notify();
            }
        } else {
            throw new IllegalMonitorStateException("Current owner is: " + owner +
                                                   ", not: " + Thread.currentThread());
        }
    }

    // MonitorCondition.Support implementation

    synchronized int releaseLock() {
        if (this.owner == Thread.currentThread()) {
            int lockCount = this.lockCount;
            assert lockCount > 0;
            this.owner = null;
            this.lockCount = 0;
            this.notify();
            return lockCount;
        } else {
            throw new IllegalMonitorStateException("Current owner is: " + owner +
                                                   ", not: " + Thread.currentThread());
        }
    }

    synchronized void regainLock(int lockCount) {
        assert lockCount > 0;
        boolean interrupted = false;
        while (true) {
            if (owner == null) {
                this.owner = Thread.currentThread();
                this.lockCount = lockCount;
                break;
            } else {
                try {
                    wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    synchronized void checkLock() {
        if (owner != Thread.currentThread()) {
            throw new IllegalMonitorStateException("Current owner is: " + owner +
                                                   ", not: " + Thread.currentThread());
        }
    }

    // Unsafe support (used in nested classes too, so package-private to avoid generated accessor methods)

    static final Unsafe U;
    static final long OWNER_OFFSET, HEAD_OFFSET, WAITER_NEXT_OFFSET;

    static {
        try {
            Field uf = Unsafe.class.getDeclaredField("theUnsafe");
            uf.setAccessible(true);
            U = (Unsafe) uf.get(null);
            OWNER_OFFSET = U.objectFieldOffset(HybridReentrantLock.class.getDeclaredField("owner"));
            HEAD_OFFSET = U.objectFieldOffset(HybridReentrantLock.class.getDeclaredField("head"));
            WAITER_NEXT_OFFSET = U.objectFieldOffset(Waiter.class.getDeclaredField("next"));
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new Error(e);
        }
    }
}
