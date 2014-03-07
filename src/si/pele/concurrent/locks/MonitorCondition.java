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
 * A {@link Condition} implementation based solely on a single
 * Java Object monitor lock (the {@link Condition} object itself).
 *
 * @author peter.levart@gmail.com
 */
class MonitorCondition implements Condition {

    private final Support lock;

    MonitorCondition(Support lock) {
        this.lock = lock;
    }

    @Override
    public void await() throws InterruptedException {
        int lockCount = 0;
        try {
            synchronized (this) {
                lockCount = lock.releaseLock();
                wait();
            }
        } finally {
            if (lockCount > 0) lock.regainLock(lockCount);
        }
    }

    @Override
    public void awaitUninterruptibly() {
        int lockCount = 0;
        boolean interrupted = false;
        try {
            synchronized (this) {
                lockCount = lock.releaseLock();
                try {
                    wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (lockCount > 0) lock.regainLock(lockCount);
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    @Override
    public long awaitNanos(long nanosTimeout) throws InterruptedException {
        long deadline = System.nanoTime() + nanosTimeout;
        int lockCount = 0;
        try {
            synchronized (this) {
                lockCount = lock.releaseLock();
                long millis = nanosTimeout / 1000_000L;
                long nanos = nanosTimeout - millis * 1000_000L;
                wait(millis, (int) nanos);
            }
        } finally {
            if (lockCount > 0) lock.regainLock(lockCount);
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
                lockCount = lock.releaseLock();
                long millis = deadline.getTime() - System.currentTimeMillis();
                if (millis <= 0) {
                    return false;
                } else {
                    wait(millis);
                    return deadline.getTime() > System.currentTimeMillis();
                }
            }
        } finally {
            if (lockCount > 0) lock.regainLock(lockCount);
        }
    }

    @Override
    public synchronized void signal() {
        lock.checkLock();
        notify();
    }

    @Override
    public synchronized void signalAll() {
        lock.checkLock();
        notifyAll();
    }


    /**
     * A package-private "interface" between {@link MonitorCondition} and
     * re-entrant {@link Lock} implementations.
     *
     * @author peter.levart@gmail.com
     */
    abstract static class Support implements Lock {

        @Override
        public final Condition newCondition() {
            return new MonitorCondition(this);
        }

        /**
         * Release the lock and return the nested lock count.
         *
         * @return nested lock count
         * @throws IllegalMonitorStateException if current thread is not owner of the lock
         */
        abstract int releaseLock() throws IllegalMonitorStateException;

        /**
         * Re-gain the lock with given nested lock count.
         *
         * @param lockCount nested lock count
         */
        abstract void regainLock(int lockCount);

        /**
         * Check that current thread is holding the lock.
         *
         * @throws IllegalMonitorStateException if current thread is not owner of the lock
         */
        abstract void checkLock() throws IllegalMonitorStateException;
    }
}
