/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.locks;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A reentrant mutual exclusion {@link Lock} implementation based solely
 * on Java Object monitor locks.
 *
 * @author peter.levart@gmail.com
 */
public class MonitorReentrantLock implements Lock {

    private Thread owner;
    private int lockCount;

    @Override
    public synchronized void lock() {
        boolean interrupted = false;
        while (true) {
            if (owner == null) {
                owner = Thread.currentThread();
                lockCount = 1;
                break;
            } else if (owner == Thread.currentThread()) {
                lockCount++;
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
}
