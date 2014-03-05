/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.locks;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

/**
 * A reentrant mutual exclusion {@link java.util.concurrent.locks.Lock} implementation based on
 * Unsafe CAS opreration and Java Object monitor locks.
 *
 * @author peter.levart@gmail.com
 */
public class HybridReentrantLock implements Lock {

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

        boolean casNext(Waiter oldNext, Waiter newNext) {
            return U.compareAndSwapObject(this, WAITER_NEXT_OFFSET, oldNext, newNext);
        }
    }

    // head of waiters list (atomically updated with CAS)
    private volatile Waiter head;

    private boolean casHead(Waiter oldHead, Waiter newHead) {
        return U.compareAndSwapObject(this, HEAD_OFFSET, oldHead, newHead);
    }

    // eventual tail of waiters list
    // (optimization so that we don't traverse the whole list on each push)
    private volatile Waiter tail;

    // a sentinel waiter put on tail of invalidated chain
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
                // re-try locking after pushing waiter on the list so that we don't miss a signal
            } else {
                LockSupport.park(this);
            }
        }
    }

    @Override
    public synchronized void lockInterruptibly() throws InterruptedException {
        while (true) {
            if (owner == null) {
                owner = Thread.currentThread();
                lockCount = 1;
                break;
            } else if (owner == Thread.currentThread()) {
                lockCount++;
                break;
            } else {
                wait();
            }
        }
    }

    @Override
    public synchronized boolean tryLock() {
        if (owner == null) {
            owner = Thread.currentThread();
            lockCount = 1;
            return true;
        } else if (owner == Thread.currentThread()) {
            lockCount++;
            return true;
        } else {
            return false;
        }
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

    /**
     * Attempts to release this lock.
     * <p/>
     * <p>If the current thread is the holder of this lock then the hold
     * count is decremented.  If the hold count is now zero then the lock
     * is released.  If the current thread is not the holder of this
     * lock then {@link IllegalMonitorStateException} is thrown.
     *
     * @throws IllegalMonitorStateException if the current thread does not
     *                                      hold this lock
     */
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

    @Override
    public Condition newCondition() {
        return new Cond();
    }

    // internal implementation

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

    private class Cond implements Condition {

        @Override
        public void await() throws InterruptedException {
            int lockCount = 0;
            try {
                synchronized (this) {
                    lockCount = releaseLock();
                    wait();
                }
            } finally {
                if (lockCount > 0) regainLock(lockCount);
            }
        }

        @Override
        public void awaitUninterruptibly() {
            int lockCount = 0;
            boolean interrupted = false;
            try {
                synchronized (this) {
                    lockCount = releaseLock();
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
            } finally {
                if (lockCount > 0) regainLock(lockCount);
                if (interrupted) Thread.currentThread().interrupt();
            }
        }

        @Override
        public long awaitNanos(long nanosTimeout) throws InterruptedException {
            long deadline = System.nanoTime() + nanosTimeout;
            int lockCount = 0;
            try {
                synchronized (this) {
                    lockCount = releaseLock();
                    long millis = nanosTimeout / 1000_000L;
                    long nanos = nanosTimeout - millis * 1000_000L;
                    wait(millis, (int) nanos);
                }
            } finally {
                if (lockCount > 0) regainLock(lockCount);
                return deadline - System.nanoTime();
            }
        }

        @Override
        public boolean await(long time, TimeUnit unit) throws InterruptedException {
            return awaitNanos(unit.toNanos(time)) > 0;
        }

        @Override
        public boolean awaitUntil(Date deadline) throws InterruptedException {
            int lockCount = 0;
            try {
                synchronized (this) {
                    lockCount = releaseLock();
                    long millis = deadline.getTime() - System.currentTimeMillis();
                    if (millis <= 0) {
                        return false;
                    } else {
                        wait(millis);
                        return deadline.getTime() > System.currentTimeMillis();
                    }
                }
            } finally {
                if (lockCount > 0) regainLock(lockCount);
            }
        }

        @Override
        public synchronized void signal() {
            checkLock();
            notify();
        }

        @Override
        public synchronized void signalAll() {
            checkLock();
            notifyAll();
        }
    }

    // Unsafe support

    private static final Unsafe U;
    private static final long OWNER_OFFSET, HEAD_OFFSET, WAITER_NEXT_OFFSET;

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
