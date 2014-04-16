/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * A re-entrant mutual exclusion {@link Lock} implementation based solely
 * on Java Object monitor locks.
 *
 * @author peter.levart@gmail.com
 */
public class MonitorReentrantLock extends MonitorCondition.Support implements Lock {

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
}
