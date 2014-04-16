/**
 * Written by Peter.Levart@gmail.com 
 * and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

import si.pele.concurrent.lock.HybridReentrantLock1;

import java.util.concurrent.locks.Lock;

/**
 * @author peter
 */
public class LockTest {

    static Thread newThread(final Lock lock, String name) {
        return new Thread(name) {
            void log(String msg) {
                System.out.println(Thread.currentThread().getName() + "[" + System.currentTimeMillis() + "]: " + msg);
            }

            @Override
            public void run() {
//                try {
//                    log("Sleeping...");
//                    Thread.sleep(1000L);
//                    log("Locking...");
                for (int i = 0; i < 1000; i++) {
                    try {
                        lock.lockInterruptibly();
                        try {
                            log("Locked");
//                        log("Sleeping...");
//                        Thread.sleep(1000L);
                        } finally {
                            lock.unlock();
                            log("Unlocked");
                        }
                    } catch (InterruptedException e) {
                        log(e.toString());
                    }
                }
//                } catch (InterruptedException e) {
//                    log("Interrupted");
//                }
            }
        };
    }

    public static void main(String[] args) throws InterruptedException {
        Lock lock = new HybridReentrantLock1();
        Thread t1 = newThread(lock, "T1");
        Thread t2 = newThread(lock, "                                     T2");
        Thread t3 = newThread(lock, "                                                                          T3");
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
    }
}
